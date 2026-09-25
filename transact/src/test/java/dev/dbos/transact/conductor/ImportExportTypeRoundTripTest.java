package dev.dbos.transact.conductor;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.Workflow;

import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A workflow exported through Conductor and imported again must come back with the same Java types.
 * Export and import travel as JSON written and read by Conductor's own mapper, so these tests take
 * that path rather than handing the in-memory records straight from export to import.
 */
public class ImportExportTypeRoundTripTest {

  /** A user type the way most are written: a plain, non-final class. */
  public static class Order {
    public String id;
    public long quantity;

    public Order() {}

    public Order(String id, long quantity) {
      this.id = id;
      this.quantity = quantity;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Order other && Objects.equals(id, other.id) && quantity == other.quantity;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, quantity);
    }
  }

  public static class OrderRejected extends RuntimeException {
    public OrderRejected() {}

    public OrderRejected(String message) {
      super(message);
    }
  }

  interface OrderService {
    Order fulfil(Order order);

    Order reject(Order order);
  }

  static class OrderServiceImpl implements OrderService {
    private final DBOS dbos;

    OrderServiceImpl(DBOS dbos) {
      this.dbos = dbos;
    }

    @Workflow(name = "fulfil")
    @Override
    public Order fulfil(Order order) {
      return dbos.runStep(() -> new Order(order.id + "-shipped", order.quantity), "ship");
    }

    @Workflow(name = "reject")
    @Override
    public Order reject(Order order) {
      throw new OrderRejected("rejected " + order.id);
    }
  }

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  OrderService service;
  SystemDatabase sysdb;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    service = dbos.registerProxy(OrderService.class, new OrderServiceImpl(dbos));
    dbos.launch();
    sysdb = DBOSTestAccess.getSystemDatabase(dbos);
  }

  /** Export through Conductor's JSON, delete the original, and import what came back. */
  private void roundTripThroughConductorJson(String workflowId) throws Exception {
    var mapper = Conductor.buildObjectMapper();
    var json =
        Conductor.serializeExportedWorkflows(sysdb.exportWorkflow(workflowId, false), mapper);
    List<ExportedWorkflow> imported = Conductor.deserializeExportedWorkflows(json, mapper);
    sysdb.deleteWorkflows(List.of(workflowId), false);
    sysdb.importWorkflow(imported);
  }

  @Test
  void workflowInputOutputAndStepOutputKeepTheirTypes() throws Exception {
    var workflowId = "roundtrip-types-" + System.currentTimeMillis();
    try (var id = new WorkflowOptions(workflowId).setContext()) {
      service.fulfil(new Order("o-1", 3L));
    }

    // Before export: the serializer keeps these types, so the checks below test the round trip.
    var before = sysdb.getWorkflowStatus(workflowId);
    assertInstanceOf(Order.class, before.output());
    assertInstanceOf(Order.class, before.input()[0]);
    assertInstanceOf(Order.class, dbos.listWorkflowSteps(workflowId).get(0).output());

    roundTripThroughConductorJson(workflowId);

    var after = sysdb.getWorkflowStatus(workflowId);
    var stepOutput = dbos.listWorkflowSteps(workflowId).get(0).output();
    assertAll(
        () -> assertInstanceOf(Order.class, after.output(), "workflow output after import"),
        () -> assertInstanceOf(Order.class, after.input()[0], "workflow input after import"),
        () -> assertInstanceOf(Order.class, stepOutput, "step output after import"),
        () ->
            assertEquals(
                new Order("o-1-shipped", 3L),
                dbos.<Order, RuntimeException>retrieveWorkflow(workflowId).getResult(),
                "getResult after import"));
  }

  @Test
  void workflowErrorKeepsItsExceptionClass() throws Exception {
    var workflowId = "roundtrip-error-" + System.currentTimeMillis();
    try (var id = new WorkflowOptions(workflowId).setContext()) {
      assertThrows(OrderRejected.class, () -> service.reject(new Order("o-2", 1L)));
    }

    // Before export: getResult rethrows the recorded exception with its own class.
    assertThrows(
        OrderRejected.class,
        () -> dbos.<Order, RuntimeException>retrieveWorkflow(workflowId).getResult());

    roundTripThroughConductorJson(workflowId);

    var thrown =
        assertThrows(
            Exception.class,
            () -> dbos.<Order, RuntimeException>retrieveWorkflow(workflowId).getResult());
    assertInstanceOf(OrderRejected.class, thrown, "workflow error after import");
    assertEquals("rejected o-2", thrown.getMessage());
  }
}
