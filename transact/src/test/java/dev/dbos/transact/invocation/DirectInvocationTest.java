package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.DBUtils.DBSettings;
import dev.dbos.transact.workflow.Timeout;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class DirectInvocationTest {
  private HawkService proxy;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  private static final DBSettings db = DBSettings.get();
  private DBOSConfig dbosConfig;
  private HikariDataSource dataSource;

  @BeforeEach
  void beforeEachTest() throws SQLException {
    db.recreate();

    dataSource = db.dataSource();
    dbosConfig = DBOSConfig.defaults("systemdbtest").withDataSource(dataSource);

    DBOS.reinitialize(dbosConfig);

    var impl = new HawkServiceImpl();
    proxy = DBOS.registerWorkflows(HawkService.class, impl);
    impl.setProxy(proxy);

    DBOS.launch();

    dataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dataSource.close();
    DBOS.shutdown();
  }

  @Test
  void directInvoke() throws Exception {

    var result = proxy.simpleWorkflow();
    assertEquals(localDate, result);

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertDoesNotThrow(() -> UUID.fromString((String) row.workflowId()));
    assertEquals("SUCCESS", row.status());
    assertEquals("simpleWorkflow", row.name());
    assertEquals("dev.dbos.transact.invocation.HawkServiceImpl", row.className());
    assertNotNull(row.output());
    assertNull(row.error());
    assertNull(row.timeoutMs());
    assertNull(row.deadlineEpochMs());
  }

  @Test
  void directInvokeSetWorkflowId() throws Exception {

    String workflowId = "directInvokeSetWorkflowId";
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      var result = proxy.simpleWorkflow();
      assertEquals(localDate, result);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(workflowId, row.workflowId());
  }

  @Test
  void directInvokeSetTimeout() throws Exception {

    var options = new WorkflowOptions().withTimeout(Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.sleepWorkflow(1);
      assertEquals(localDate, result);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(10000L, row.timeoutMs());
    assertNotNull(row.deadlineEpochMs());
  }

  @Test
  void directInvokeSetZeroTimeout() throws Exception {

    var options = new WorkflowOptions().withTimeout(Timeout.none());
    try (var _o = options.setContext()) {
      var result = proxy.sleepWorkflow(1);
      assertEquals(localDate, result);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertNull(row.timeoutMs());
    assertNull(row.deadlineEpochMs());
  }

  @Test
  void directInvokeSetWorkflowIdAndTimeout() throws Exception {

    String workflowId = "directInvokeSetWorkflowIdAndTimeout";
    var options = new WorkflowOptions(workflowId).withTimeout(Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.sleepWorkflow(1);
      assertEquals(localDate, result);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(workflowId, row.workflowId());
    assertEquals(10000L, row.timeoutMs());
    assertNotNull(row.deadlineEpochMs());
  }

  @Test
  void directInvokeTimeoutCancellation() throws Exception {

    var options = new WorkflowOptions().withTimeout(Duration.ofSeconds(1));
    try (var _o = options.setContext()) {
      assertThrows(DBOSAwaitedWorkflowCancelledException.class, () -> proxy.sleepWorkflow(10L));
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals("CANCELLED", row.status());
    assertNull(row.output());
    assertNull(row.error());
  }

  @Test
  void directInvokeTimeoutDeadline() throws Exception {

    var options =
        new WorkflowOptions().withDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 1000));
    try (var _o = options.setContext()) {
      assertThrows(DBOSAwaitedWorkflowCancelledException.class, () -> proxy.sleepWorkflow(10L));
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals("CANCELLED", row.status());
    assertNull(row.output());
    assertNull(row.error());
  }

  @Test
  void directInvokeSetWorkflowIdTimeoutCancellation() throws Exception {

    var workflowId = "directInvokeSetWorkflowIdTimeoutCancellation";
    var options = new WorkflowOptions(workflowId).withTimeout(Duration.ofSeconds(1));
    try (var _o = options.setContext()) {
      assertThrows(DBOSAwaitedWorkflowCancelledException.class, () -> proxy.sleepWorkflow(10L));
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(workflowId, row.workflowId());
    assertEquals("CANCELLED", row.status());
    assertNull(row.output());
    assertNull(row.error());
  }

  @Test
  void directInvokeParent() throws Exception {

    var result = proxy.parentWorkflow();
    assertEquals(localDate, result);

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(2, rows.size());
    var row0 = rows.get(0);
    var row1 = rows.get(1);
    assertDoesNotThrow(() -> UUID.fromString(row0.workflowId()));
    assertEquals(row0.workflowId() + "-0", row1.workflowId());
    assertEquals("SUCCESS", row0.status());
    assertEquals("SUCCESS", row1.status());
    assertEquals("parentWorkflow", row0.name());
    assertEquals("simpleWorkflow", row1.name());
    assertEquals(row0.output(), row1.output());
    assertNull(row0.timeoutMs());
    assertNull(row1.timeoutMs());
    assertNull(row0.deadlineEpochMs());
    assertNull(row1.deadlineEpochMs());

    var steps = DBUtils.getStepRows(dataSource, row0.workflowId());
    assertEquals(1, steps.size());
    var step = steps.get(0);
    assertEquals(row0.workflowId(), step.workflowId());
    assertEquals(0, step.functionId());
    assertNull(step.output());
    assertNull(step.error());
    assertEquals("simpleWorkflow", step.functionName());
    assertEquals(row1.workflowId(), step.childWorkflowId());
  }

  @Test
  void directInvokeParentStartWorkflow() throws Exception {
    var result = proxy.parentStartWorkflow();
    assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(2, rows.size());
    var row0 = rows.get(0);
    var row1 = rows.get(1);
    assertDoesNotThrow(() -> UUID.fromString(row0.workflowId()));
    assertEquals(row0.workflowId() + "-0", row1.workflowId());
    assertEquals("SUCCESS", row0.status());
    assertEquals("SUCCESS", row1.status());
    assertEquals("parentStartWorkflow", row0.name());
    assertEquals("simpleWorkflow", row1.name());
    assertEquals(row0.output(), row1.output());
    assertNull(row0.timeoutMs());
    assertNull(row1.timeoutMs());
    assertNull(row0.deadlineEpochMs());
    assertNull(row1.deadlineEpochMs());

    var steps = DBUtils.getStepRows(dataSource, row0.workflowId());
    assertEquals(2, steps.size());
    var step = steps.get(0);
    var gr = steps.get(1);
    assertEquals(row0.workflowId(), step.workflowId());
    assertEquals(0, step.functionId());
    assertNull(step.output());
    assertNull(step.error());
    assertEquals("simpleWorkflow", step.functionName());
    assertEquals(row1.workflowId(), step.childWorkflowId());
    assertEquals("DBOS.getResult", gr.functionName());
  }

  @Test
  void directInvokeParentSetWorkflowId() throws Exception {

    String workflowId = "directInvokeParentSetWorkflowId";
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      var result = proxy.parentWorkflow();
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(2, rows.size());
    var row0 = rows.get(0);
    var row1 = rows.get(1);
    assertEquals(workflowId, row0.workflowId());
    assertEquals(workflowId + "-0", row1.workflowId());
  }

  @Test
  void directInvokeParentSetTimeout() throws Exception {

    var options = new WorkflowOptions().withTimeout(Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.parentSleepWorkflow(null, 1);
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(2, rows.size());
    var row0 = rows.get(0);
    var row1 = rows.get(1);

    assertEquals(10000L, row0.timeoutMs());
    assertEquals(10000L, row1.timeoutMs());
    assertNotNull(row0.deadlineEpochMs());
    assertNotNull(row1.deadlineEpochMs());
    assertEquals(row0.deadlineEpochMs(), row1.deadlineEpochMs());
  }

  @Test
  void directInvokeParentSetTimeoutParent() throws Exception {

    var result = proxy.parentSleepWorkflow(5L, 1);
    assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(2, rows.size());
    var row0 = rows.get(0);
    var row1 = rows.get(1);
    assertNull(row0.timeoutMs());
    assertNull(row0.deadlineEpochMs());
    assertEquals(5000L, row1.timeoutMs());
    assertNotNull(row1.deadlineEpochMs());
  }

  @Test
  void directInvokeParentSetTimeoutParent2() throws Exception {

    var options = new WorkflowOptions().withTimeout(Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.parentSleepWorkflow(5L, 1);
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(2, rows.size());
    var row0 = rows.get(0);
    var row1 = rows.get(1);

    assertEquals(10000L, row0.timeoutMs());
    assertNotNull(row0.deadlineEpochMs());

    assertEquals(5000L, row1.timeoutMs());
    assertNotNull(row1.deadlineEpochMs());
  }

  @Test
  void directInvokeParentSetTimeoutParent3() throws Exception {

    var options = new WorkflowOptions().withTimeout(Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.parentSleepWorkflow(0L, 1);
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(2, rows.size());
    var row0 = rows.get(0);
    var row1 = rows.get(1);

    assertEquals(10000L, row0.timeoutMs());
    assertNotNull(row0.deadlineEpochMs());

    assertNull(row1.timeoutMs());
    assertNull(row1.deadlineEpochMs());
  }

  @Test
  void invokeWorkflowFromStepThrows() throws Exception {
    var ise = assertThrows(IllegalStateException.class, () -> proxy.illegalWorkflow());
    assertEquals("cannot invoke a workflow from a step", ise.getMessage());

    var wfs = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, wfs.size());
    var wf = wfs.get(0);
    assertNotNull(wf.workflowId());

    var steps = DBOS.listWorkflowSteps(wf.workflowId());
    assertEquals(1, steps.size());
    var step = steps.get(0);
    assertEquals(0, step.functionId());
    assertNull(step.output());
    assertEquals("cannot invoke a workflow from a step", step.error().message());
    assertEquals("cannot invoke a workflow from a step", step.error().throwable().getMessage());
    assertEquals("illegalStep", step.functionName());
  }

  @Test
  void directInvokeStep() throws Exception {
    var result = proxy.stepWorkflow();
    assertNotNull(result);

    var wfs = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, wfs.size());
    var wf = wfs.get(0);
    assertNotNull(wf.workflowId());

    var steps = DBUtils.getStepRows(dataSource, wf.workflowId());
    assertEquals(1, steps.size());
    var step = steps.get(0);
    assertEquals(0, step.functionId());
    assertNotNull(step.output());
    assertNull(step.error());
    assertEquals("nowStep", step.functionName());
  }

  @Test
  void directInvokeParentSetParentTimeout() throws Exception {

    var options = new WorkflowOptions().withTimeout(Duration.ofSeconds(10));
    try (var _o = options.setContext()) {
      var result = proxy.parentWorkflow();
      assertEquals(LocalDate.now().format(DateTimeFormatter.ISO_DATE), result);
    }

    var table = DBUtils.getWorkflowRows(dataSource);
    assertEquals(2, table.size());
    var row0 = table.get(0);
    var row1 = table.get(1);
    assertEquals(10000L, row0.timeoutMs());
    assertEquals(10000L, row1.timeoutMs());
    assertNotNull(row0.deadlineEpochMs());
    assertNotNull(row1.deadlineEpochMs());
    assertEquals(row0.deadlineEpochMs(), row1.deadlineEpochMs());
  }
}
