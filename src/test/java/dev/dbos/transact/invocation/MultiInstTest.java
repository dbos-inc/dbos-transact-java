package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class MultiInstTest {
  private static DBOSConfig dbosConfig;
  private DBOS dbos;
  HawkServiceImpl himpl;
  BearServiceImpl bimpla;
  BearServiceImpl bimpl1;
  private HawkService hproxy;
  private BearService bproxya;
  private BearService bproxy1;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  @BeforeAll
  static void onetimeSetup() throws Exception {
    dbosConfig =
        new DBOSConfig.Builder()
            .appName("systemdbtest")
            .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    dbos = DBOS.initialize(dbosConfig);
    himpl = new HawkServiceImpl();
    bimpla = new BearServiceImpl();
    bimpl1 = new BearServiceImpl();
    dbos.Queue("testQueue").build();

    hproxy = dbos.registerWorkflows(HawkService.class, himpl);
    himpl.setProxy(hproxy);

    bproxya = dbos.registerWorkflows(BearService.class, bimpla, "A");
    bimpla.setProxy(bproxya);

    bproxy1 = dbos.registerWorkflows(BearService.class, bimpl1, "1");
    bimpl1.setProxy(bproxy1);

    dbos.launch();
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dbos.shutdown();
  }

  @Test
  void startWorkflow() throws Exception {
    var bhandlea =
        dbos.startWorkflow(
            () -> {
              return bproxya.stepWorkflow();
            });
    var bresulta = bhandlea.getResult();
    assertEquals(
        localDate,
        bresulta
            .atZone(ZoneId.systemDefault()) // or another zone
            .toLocalDate()
            .format(DateTimeFormatter.ISO_DATE));
    assertEquals(1, bimpla.nWfCalls);

    var bhandle1 =
        dbos.startWorkflow(
            () -> {
              return bproxy1.stepWorkflow();
            });
    var bresult1 = bhandle1.getResult();
    assertEquals(
        localDate,
        bresult1
            .atZone(ZoneId.systemDefault()) // or another zone
            .toLocalDate()
            .format(DateTimeFormatter.ISO_DATE));
    assertEquals(1, bimpl1.nWfCalls);

    var hhandle =
        dbos.startWorkflow(
            () -> {
              return hproxy.stepWorkflow();
            });
    var hresult = hhandle.getResult();
    assertEquals(
        localDate,
        hresult
            .atZone(ZoneId.systemDefault()) // or another zone
            .toLocalDate()
            .format(DateTimeFormatter.ISO_DATE));

    var browsa =
        dbos.listWorkflows(
            new ListWorkflowsInput.Builder().workflowID(bhandlea.getWorkflowId()).build());
    assertEquals(1, browsa.size());
    var browa = browsa.get(0);
    assertEquals(bhandlea.getWorkflowId(), browa.workflowId());
    assertEquals("stepWorkflow", browa.name());
    assertEquals("A", browa.instanceName());
    assertEquals("dev.dbos.transact.invocation.BearServiceImpl", browa.className());
    assertEquals("SUCCESS", browa.status());

    var brows1 =
        dbos.listWorkflows(
            new ListWorkflowsInput.Builder().workflowID(bhandle1.getWorkflowId()).build());
    assertEquals(1, brows1.size());
    var brow1 = brows1.get(0);
    assertEquals(bhandle1.getWorkflowId(), brow1.workflowId());
    assertEquals("stepWorkflow", brow1.name());
    assertEquals("1", brow1.instanceName());
    assertEquals("dev.dbos.transact.invocation.BearServiceImpl", brow1.className());
    assertEquals("SUCCESS", brow1.status());

    var hrows =
        dbos.listWorkflows(
            new ListWorkflowsInput.Builder().workflowID(hhandle.getWorkflowId()).build());
    assertEquals(1, hrows.size());
    var hrow = hrows.get(0);
    assertEquals(hhandle.getWorkflowId(), hrow.workflowId());
    assertEquals("stepWorkflow", hrow.name());
    assertEquals("dev.dbos.transact.invocation.HawkServiceImpl", hrow.className());
    assertEquals("", hrow.instanceName());
    assertEquals("SUCCESS", hrow.status());

    // All 3 w/ the same WF name
    var allrows =
        dbos.listWorkflows(new ListWorkflowsInput.Builder().workflowName("stepWorkflow").build());
    assertEquals(3, allrows.size());

    // 2 from BSI
    var brows =
        dbos.listWorkflows(
            new ListWorkflowsInput.Builder()
                .workflowName("stepWorkflow")
                .className("dev.dbos.transact.invocation.BearServiceImpl")
                .build());
    assertEquals(2, brows.size());

    // 2 from BSI
    var browsjust1 =
        dbos.listWorkflows(
            new ListWorkflowsInput.Builder()
                .workflowName("stepWorkflow")
                .className("dev.dbos.transact.invocation.BearServiceImpl")
                .instanceName("1")
                .build());
    assertEquals(1, browsjust1.size());
  }

  private static final String dbUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys";
  private static final String dbUser = "postgres";
  private static final String dbPassword = System.getenv("PGPASSWORD");

  @Test
  public void enqueueForSpecificInstance() throws Exception {
    try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
      var options =
          new DBOSClient.EnqueueOptions(
                  "dev.dbos.transact.invocation.BearServiceImpl", "stepWorkflow", "testQueue")
              .withInstanceName("A");
      var handle = client.<Instant, RuntimeException>enqueueWorkflow(options, new Object[] {});

      var result = handle.getResult();
      assertEquals(
          localDate,
          result
              .atZone(ZoneId.systemDefault()) // or another zone
              .toLocalDate()
              .format(DateTimeFormatter.ISO_DATE));

      assertEquals(1, bimpla.nWfCalls);
      assertEquals(0, bimpl1.nWfCalls);

      var stat = client.getWorkflowStatus(handle.getWorkflowId());
      assertEquals(
          "SUCCESS",
          stat.orElseThrow(() -> new AssertionError("Workflow status not found")).status());

      var dataSource = SystemDatabase.createDataSource(dbosConfig);
      DBUtils.setWorkflowState(dataSource, handle.getWorkflowId(), WorkflowState.PENDING.name());
      stat = client.getWorkflowStatus(handle.getWorkflowId());
      assertEquals(
          "PENDING",
          stat.orElseThrow(() -> new AssertionError("Workflow status not found")).status());

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
      var eh = dbosExecutor.executeWorkflowById(handle.getWorkflowId());
      eh.getResult();
      stat = client.getWorkflowStatus(handle.getWorkflowId());
      assertEquals(
          "SUCCESS",
          stat.orElseThrow(() -> new AssertionError("Workflow status not found")).status());
      assertEquals(0, bimpl1.nWfCalls);
      assertEquals(2, bimpla.nWfCalls);
    }
  }
}
