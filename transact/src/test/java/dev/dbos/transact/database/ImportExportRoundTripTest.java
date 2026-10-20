package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DebouncedRows;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A workflow exported and imported again must come back as it was: with the same Java types, still
 * debounced if it was, and with its payloads where this release writes them.
 */
public class ImportExportRoundTripTest {

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
  @AutoClose HikariDataSource dataSource;
  OrderService service;
  SystemDatabase sysdb;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    service = dbos.registerProxy(OrderService.class, new OrderServiceImpl(dbos));
    dbos.launch();
    sysdb = DBOSTestAccess.getSystemDatabase(dbos);
    dataSource = pgContainer.dataSource();
  }

  /** Export the workflow, delete the original, and import the export. */
  private void roundTrip(String workflowId) throws Exception {
    List<ExportedWorkflow> exported = sysdb.exportWorkflow(workflowId, false);
    sysdb.deleteWorkflows(List.of(workflowId), false);
    sysdb.importWorkflow(exported);
  }

  /**
   * A debounced workflow still waiting must come back debounced, with its debounce deadline, or
   * later calls on its key stop coalescing into it and its key outlives its release.
   */
  @Test
  void aWaitingDebouncedWorkflowComesBackDebounced() throws Exception {
    var debouncer = dbos.<Order>debouncer().withDebounceTimeout(Duration.ofMinutes(10));
    var handle =
        debouncer.debounce("rt", Duration.ofMinutes(5), () -> service.fulfil(new Order("o-1", 1)));
    var before = DebouncedRows.read(dataSource, handle.workflowId());
    assertTrue(before.isDebounced());
    assertNotNull(before.debounceDeadlineEpochMs());

    roundTrip(handle.workflowId());

    var after = DebouncedRows.read(dataSource, handle.workflowId());
    assertEquals(WorkflowState.DELAYED.name(), after.status());
    assertTrue(after.isDebounced());
    assertEquals(before.debounceDeadlineEpochMs(), after.debounceDeadlineEpochMs());
    assertEquals(before.delayUntilEpochMs(), after.delayUntilEpochMs());
    assertEquals(before.deduplicationId(), after.deduplicationId());
    // And it still coalesces: the next call on the key bounces the imported row.
    var again =
        debouncer.debounce("rt", Duration.ofMinutes(5), () -> service.fulfil(new Order("o-2", 2)));
    assertEquals(handle.workflowId(), again.workflowId());
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

    roundTrip(workflowId);

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

    roundTrip(workflowId);

    var thrown =
        assertThrows(
            Exception.class,
            () -> dbos.<Order, RuntimeException>retrieveWorkflow(workflowId).getResult());
    assertInstanceOf(OrderRejected.class, thrown, "workflow error after import");
    assertEquals("rejected o-2", thrown.getMessage());
  }

  /**
   * A workflow whose payloads are in the workflow_status columns exports them, and imports them
   * into workflow_input and workflow_output, where this release writes payloads.
   *
   * <p>The status columns hold the payloads of a workflow an earlier release wrote.
   */
  @Test
  void payloadsReadFromTheStatusRowImportIntoThePayloadTables() throws Exception {
    var workflowId = "roundtrip-status-row-" + System.currentTimeMillis();
    try (var id = new WorkflowOptions(workflowId).setContext()) {
      service.fulfil(new Order("o-3", 5L));
    }

    // Move the payloads to where an earlier release wrote them.
    execute(
        """
        UPDATE dbos.workflow_status ws SET inputs = wi.inputs
        FROM dbos.workflow_input wi
        WHERE wi.workflow_uuid = ws.workflow_uuid AND ws.workflow_uuid = ?
        """,
        workflowId);
    execute(
        """
        UPDATE dbos.workflow_status ws SET output = wo.output, error = wo.error
        FROM dbos.workflow_output wo
        WHERE wo.workflow_uuid = ws.workflow_uuid AND ws.workflow_uuid = ?
        """,
        workflowId);
    execute("DELETE FROM dbos.workflow_input WHERE workflow_uuid = ?", workflowId);
    execute("DELETE FROM dbos.workflow_output WHERE workflow_uuid = ?", workflowId);
    assertInstanceOf(Order.class, sysdb.getWorkflowStatus(workflowId).output());

    roundTrip(workflowId);

    var after = sysdb.getWorkflowStatus(workflowId);
    assertAll(
        () -> assertInstanceOf(Order.class, after.input()[0], "workflow input after import"),
        () -> assertInstanceOf(Order.class, after.output(), "workflow output after import"),
        () -> assertNull(statusColumn(workflowId, "inputs"), "inputs in workflow_status"),
        () -> assertNull(statusColumn(workflowId, "output"), "output in workflow_status"),
        () -> assertTrue(hasRow("workflow_input", workflowId), "workflow_input row"),
        () -> assertTrue(hasRow("workflow_output", workflowId), "workflow_output row"));
  }

  private void execute(String sql, String workflowId) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.executeUpdate();
    }
  }

  /** A payload column of the workflow's status row, read raw rather than through both shapes. */
  private String statusColumn(String workflowId, String column) throws SQLException {
    var sql = "SELECT %s FROM dbos.workflow_status WHERE workflow_uuid = ?".formatted(column);
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "no status row for " + workflowId);
        return rs.getString(1);
      }
    }
  }

  private boolean hasRow(String table, String workflowId) throws SQLException {
    var sql = "SELECT 1 FROM dbos.%s WHERE workflow_uuid = ?".formatted(table);
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        return rs.next();
      }
    }
  }
}
