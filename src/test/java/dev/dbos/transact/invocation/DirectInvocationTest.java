package dev.dbos.transact.invocation;

import static dev.dbos.transact.utils.Assertions.assertKeyIsNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.CancellationException;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DirectInvocationTest {
  private static DBOSConfig dbosConfig;
  private DBOS dbos;
  private HawkService proxy;
  private HikariDataSource dataSource;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  @BeforeAll
  static void onetimeSetup() throws Exception {

    dbosConfig =
        new DBOSConfig.Builder()
            .name("systemdbtest")
            .dbHost("localhost")
            .dbPort(5432)
            .dbUser("postgres")
            .sysDbName("dbos_java_sys")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    dbos = DBOS.initialize(dbosConfig);
    var impl = new HawkServiceImpl();
    proxy =
        dbos.<HawkService>Workflow().interfaceClass(HawkService.class).implementation(impl).build();
    impl.setProxy(proxy);

    dbos.launch();

    dataSource = SystemDatabase.createDataSource(dbosConfig, null);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dataSource.close();
    dbos.shutdown();
  }

  @Test
  void directInvoke() {

    var result = proxy.simpleWorkflow();
    assertEquals(localDate, result);

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(1, table.size());
    var row = table.get(0);
    assertDoesNotThrow(() -> UUID.fromString((String) row.get("workflow_uuid")));
    assertEquals("SUCCESS", row.get("status"));
    assertEquals("simpleWorkflow", row.get("name"));
    assertEquals("dev.dbos.transact.invocation.HawkServiceImpl", row.get("class_name"));
    assertNotNull(row.get("output"));
    assertKeyIsNull(row, "error");
    assertKeyIsNull(row, "workflow_timeout_ms");
    assertKeyIsNull(row, "workflow_deadline_epoch_ms");
  }

  @Test
  void directInvokeSetWorkflowId() {

    String workflowId = "directInvokeSetWorkflowId";
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      var result = proxy.simpleWorkflow();
      assertEquals(localDate, result);
    }

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(1, table.size());
    var row = table.get(0);
    assertEquals(workflowId, row.get("workflow_uuid"));
  }

  @Test
  void directInvokeSetTimeout() {

    var options = new WorkflowOptions(Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.sleepWorkflow(1);
      assertEquals(localDate, result);
    }

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(1, table.size());
    var row = table.get(0);
    assertEquals(10000L, row.get("workflow_timeout_ms"));
    assertNotNull(row.get("workflow_deadline_epoch_ms"));
  }

  @Test
  void directInvokeSetZeroTimeout() {

    var options = new WorkflowOptions(Duration.ZERO);
    try (var _o = options.setContext()) {
      var result = proxy.sleepWorkflow(1);
      assertEquals(localDate, result);
    }

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(1, table.size());
    var row = table.get(0);
    assertKeyIsNull(row, "workflow_timeout_ms");
    assertKeyIsNull(row, "workflow_deadline_epoch_ms");
  }

  @Test
  void directInvokeSetWorkflowIdAndTimeout() {

    String workflowId = "directInvokeSetWorkflowIdAndTimeout";
    var options = new WorkflowOptions(workflowId, Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.sleepWorkflow(1);
      assertEquals(localDate, result);
    }

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(1, table.size());
    var row = table.get(0);
    assertEquals(workflowId, row.get("workflow_uuid"));
    assertEquals(10000L, row.get("workflow_timeout_ms"));
    assertNotNull(row.get("workflow_deadline_epoch_ms"));
  }

  @Test
  void directInvokeTimeoutCancellation() {

    var options = new WorkflowOptions(Duration.ofSeconds(1));
    try (var _o = options.setContext()) {
      assertThrows(CancellationException.class, () -> proxy.sleepWorkflow(10L));
    }

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(1, table.size());
    var row = table.get(0);
    assertEquals("CANCELLED", row.get("status"));
    assertKeyIsNull(row, "output");
    assertKeyIsNull(row, "error");
  }

  @Test
  void directInvokeSetWorkflowIdTimeoutCancellation() {

    var workflowId = "directInvokeSetWorkflowIdTimeoutCancellation";
    var options = new WorkflowOptions(workflowId, Duration.ofSeconds(1));
    try (var _o = options.setContext()) {
      assertThrows(CancellationException.class, () -> proxy.sleepWorkflow(10L));
    }

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(1, table.size());
    var row = table.get(0);
    assertEquals(workflowId, row.get("workflow_uuid"));
    assertEquals("CANCELLED", row.get("status"));
    assertKeyIsNull(row, "output");
    assertKeyIsNull(row, "error");
  }

  @Test
  void directInvokeParent() {

    var result = proxy.parentWorkflow();
    assertEquals(localDate, result);

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(2, table.size());
    var row0 = table.get(0);
    var row1 = table.get(1);
    assertDoesNotThrow(() -> UUID.fromString((String) row0.get("workflow_uuid")));
    assertEquals(row0.get("workflow_uuid") + "-0", row1.get("workflow_uuid"));
    assertEquals("SUCCESS", row0.get("status"));
    assertEquals("SUCCESS", row1.get("status"));
    assertEquals("parentWorkflow", row0.get("name"));
    assertEquals("simpleWorkflow", row1.get("name"));
    assertEquals(row0.get("output"), row1.get("output"));
    assertKeyIsNull(row0, "workflow_timeout_ms");
    assertKeyIsNull(row1, "workflow_timeout_ms");
    assertKeyIsNull(row0, "workflow_deadline_epoch_ms");
    assertKeyIsNull(row1, "workflow_deadline_epoch_ms");
  }

  @Test
  void directInvokeParentSetWorkflowId() {

    String workflowId = "directInvokeParentSetWorkflowId";
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      var result = proxy.parentWorkflow();
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
    }

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(2, table.size());
    var row0 = table.get(0);
    var row1 = table.get(1);
    assertEquals(workflowId, row0.get("workflow_uuid"));
    assertEquals(workflowId + "-0", row1.get("workflow_uuid"));
  }

  @Test
  void directInvokeParentSetTimeout() {

    var options = new WorkflowOptions(Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.parentSleepWorkflow(null, 1);
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
    }

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(2, table.size());
    var row0 = table.get(0);
    var row1 = table.get(1);
    assertEquals(10000L, row0.get("workflow_timeout_ms"));
    assertEquals(10000L, row1.get("workflow_timeout_ms"));
    assertNotNull(row0.get("workflow_deadline_epoch_ms"));
    assertNotNull(row1.get("workflow_deadline_epoch_ms"));
    assertEquals(row0.get("workflow_deadline_epoch_ms"), row1.get("workflow_deadline_epoch_ms"));
  }

  @Test
  void directInvokeParentSetTimeoutParent() {

    var result = proxy.parentSleepWorkflow(5L, 1);
    assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(2, table.size());
    var row0 = table.get(0);
    var row1 = table.get(1);
    assertKeyIsNull(row0, "workflow_timeout_ms");
    assertKeyIsNull(row0, "workflow_deadline_epoch_ms");

    assertEquals(5000L, row1.get("workflow_timeout_ms"));
    assertNotNull(row1.get("workflow_deadline_epoch_ms"));
  }

  @Test
  void directInvokeParentSetTimeoutParent2() {

    var options = new WorkflowOptions(Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.parentSleepWorkflow(5L, 1);
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
    }

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(2, table.size());
    var row0 = table.get(0);
    var row1 = table.get(1);
    assertEquals(10000L, row0.get("workflow_timeout_ms"));
    assertNotNull(row0.get("workflow_deadline_epoch_ms"));

    assertEquals(5000L, row1.get("workflow_timeout_ms"));
    assertNotNull(row1.get("workflow_deadline_epoch_ms"));
  }

  @Test
  void directInvokeParentSetTimeoutParent3() {

    var options = new WorkflowOptions(Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.parentSleepWorkflow(0L, 1);
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
    }

    var table = DBUtils.dumpWfStatus(dataSource);
    assertEquals(2, table.size());
    var row0 = table.get(0);
    var row1 = table.get(1);
    assertEquals(10000L, row0.get("workflow_timeout_ms"));
    assertNotNull(row0.get("workflow_deadline_epoch_ms"));

    assertKeyIsNull(row1, "workflow_timeout_ms");
    assertKeyIsNull(row1, "workflow_deadline_epoch_ms");
  }

  // @Test
  // void directInvokeParentSetParentTimeout() {

  //   var options = new WorkflowOptions(Duration.ofSeconds(10));
  //   try (var _o = options.setContext()) {
  //     var result = proxy.parentWorkflow();
  //     assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
  //   }

  //   var table = DBUtils.dumpWfStatus(dataSource);
  //   assertEquals(2, table.size());
  //   var row0 = table.get(0);
  //   var row1 = table.get(1);
  //   assertEquals(10000L, row0.get("workflow_timeout_ms"));
  //   assertEquals(10000L, row1.get("workflow_timeout_ms"));
  //   assertNotNull(row0.get("workflow_deadline_epoch_ms"));
  //   assertNotNull(row1.get("workflow_deadline_epoch_ms"));
  //   assertEquals(row0.get("workflow_deadline_epoch_ms"), row1.get("workflow_deadline_epoch_ms"));
  // }

  // @Test
  // void directInvokeParentTimeout() {

  //   var impl = new HawkServiceImpl();
  //   var proxy =
  //
  // dbos.<HawkService>Workflow().interfaceClass(HawkService.class).implementation(impl).build();
  //   impl.setProxy(proxy);

  //   dbos.launch();

  //   var options = new WorkflowOptions(Duration.ofSeconds(1));
  //   try (var _o = options.setContext()) {
  //     assertThrows(CancellationException.class, () -> proxy.parentSleepWorkflow(null, 10L));
  //   }
  // }
}
