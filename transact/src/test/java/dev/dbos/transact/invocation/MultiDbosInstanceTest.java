package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DbSetupTestBase;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Workflow;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface TestService {
  String testWorkflow(String name);
}

class TestServiceImpl implements TestService {
  private final DBOS.Instance dbos;

  public TestServiceImpl(DBOS.Instance instance) {
    this.dbos = instance;
  }

  @Workflow
  public String testWorkflow(String name) {
    var today = dbos.runStep(() -> LocalDate.now(), "todaysDate");
    return String.format("Hello %s, today is %s", name, today.format(DateTimeFormatter.ISO_DATE));
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class MultiDbosInstanceTest extends DbSetupTestBase {

  private static DBOSConfig dbosConfigA;
  private DBOS.Instance dbosA;
  private TestService proxyA;
  private TestServiceImpl implA;
  private Queue queueA;

  private static DBOSConfig dbosConfigB;
  private DBOS.Instance dbosB;
  private TestServiceImpl implB;
  private TestService proxyB;
  private Queue queueB;

  @BeforeEach
  void beforeEachTest() throws Exception {
    dbosConfigA =
        createConfigFromEnv("MultiDbosInstanceTestA")
            .withDatabaseUrl(getJdbcUrl("dbos_java_multi_a"));
    dbosConfigB =
        createConfigFromEnv("MultiDbosInstanceTestB")
            .withDatabaseUrl(getJdbcUrl("dbos_java_multi_b"));
    DBUtils.recreateDB(dbosConfigA);
    dbosA = new DBOS.Instance(dbosConfigA);
    implA = new TestServiceImpl(dbosA);
    proxyA = dbosA.registerWorkflows(TestService.class, implA);
    queueA = new Queue("queueA");
    dbosA.registerQueue(queueA);

    dbosA.launch();

    DBUtils.recreateDB(dbosConfigB);
    dbosB = new DBOS.Instance(dbosConfigB);
    implB = new TestServiceImpl(dbosB);
    proxyB = dbosB.registerWorkflows(TestService.class, implB);
    queueB = new Queue("queueB");
    dbosB.registerQueue(queueB);
    dbosB.launch();
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dbosA.shutdown();
    dbosB.shutdown();
  }

  @Test
  public void testDirectMultipleInstances() throws Exception {
    var wfidA = UUID.randomUUID().toString();
    String resultA;
    try (var o = new WorkflowOptions(wfidA).setContext()) {
      resultA = proxyA.testWorkflow("hawk");
    }

    var wfidB = UUID.randomUUID().toString();
    String resultB;
    try (var o = new WorkflowOptions(wfidB).setContext()) {
      resultB = proxyB.testWorkflow("bear");
    }

    String formattedCurrentDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
    assertEquals("Hello hawk, today is " + formattedCurrentDate, resultA);
    assertEquals("Hello bear, today is " + formattedCurrentDate, resultB);

    var rowsA = DBUtils.getWorkflowRows(dbosConfigA);
    var rowsB = DBUtils.getWorkflowRows(dbosConfigB);
    assertEquals(1, rowsA.size());
    assertEquals(1, rowsB.size());
    assertEquals(wfidA, rowsA.get(0).workflowId());
    assertEquals(wfidB, rowsB.get(0).workflowId());
  }

  @Test
  public void testEnqueueMultipleInstances() throws Exception {
    var handleA =
        dbosA.startWorkflow(() -> proxyA.testWorkflow("hawk"), new StartWorkflowOptions(queueA));
    var handleB =
        dbosB.startWorkflow(() -> proxyB.testWorkflow("bear"), new StartWorkflowOptions(queueB));

    String formattedCurrentDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
    assertEquals("Hello hawk, today is " + formattedCurrentDate, handleA.getResult());
    assertEquals("Hello bear, today is " + formattedCurrentDate, handleB.getResult());

    var rowsA = DBUtils.getWorkflowRows(dbosConfigA);
    var rowsB = DBUtils.getWorkflowRows(dbosConfigB);
    assertEquals(1, rowsA.size());
    assertEquals(1, rowsB.size());
    assertEquals(handleA.workflowId(), rowsA.get(0).workflowId());
    assertEquals(handleB.workflowId(), rowsB.get(0).workflowId());
  }

  @Test
  public void cantStartOnWrongInstance() throws Exception {
    assertThrows(
        IllegalStateException.class, () -> dbosA.startWorkflow(() -> proxyB.testWorkflow("bear")));
  }

  @Test
  public void cantEnqueueOnWrongQueueInstance() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbosA.startWorkflow(
                () -> proxyA.testWorkflow("hawk"), new StartWorkflowOptions(queueB)));
  }
}
