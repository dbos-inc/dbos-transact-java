package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class MultiClassInstanceTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  HawkServiceImpl himpl;
  BearServiceImpl bimpla;
  BearServiceImpl bimpl1;
  private HawkService hproxy;
  private BearService bproxya;
  private BearService bproxy1;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  @BeforeEach
  void beforeEachTest() {
    var dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();

    himpl = new HawkServiceImpl(dbos);
    bimpla = new BearServiceImpl(dbos);
    bimpl1 = new BearServiceImpl(dbos);
    dbos.registerQueue(new Queue("testQueue"));

    hproxy = dbos.registerWorkflows(HawkService.class, himpl);
    himpl.setProxy(hproxy);

    bproxya = dbos.registerWorkflows(BearService.class, bimpla, "A");
    bimpla.setProxy(bproxya);

    bproxy1 = dbos.registerWorkflows(BearService.class, bimpl1, "1");
    bimpl1.setProxy(bproxy1);

    dbos.launch();
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

    var browsa = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowId(bhandlea.workflowId()));
    assertEquals(1, browsa.size());
    var browa = browsa.get(0);
    assertEquals(bhandlea.workflowId(), browa.workflowId());
    assertEquals("stepWorkflow", browa.workflowName());
    assertEquals("A", browa.instanceName());
    assertEquals("dev.dbos.transact.invocation.BearServiceImpl", browa.className());
    assertEquals("SUCCESS", browa.status());

    var brows1 = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowId(bhandle1.workflowId()));
    assertEquals(1, brows1.size());
    var brow1 = brows1.get(0);
    assertEquals(bhandle1.workflowId(), brow1.workflowId());
    assertEquals("stepWorkflow", brow1.workflowName());
    assertEquals("1", brow1.instanceName());
    assertEquals("dev.dbos.transact.invocation.BearServiceImpl", brow1.className());
    assertEquals("SUCCESS", brow1.status());

    var hrows = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowId(hhandle.workflowId()));
    assertEquals(1, hrows.size());
    var hrow = hrows.get(0);
    assertEquals(hhandle.workflowId(), hrow.workflowId());
    assertEquals("stepWorkflow", hrow.workflowName());
    assertEquals("dev.dbos.transact.invocation.HawkServiceImpl", hrow.className());
    assertEquals("", hrow.instanceName());
    assertEquals("SUCCESS", hrow.status());

    // All 3 w/ the same WF name
    var allrows = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowName("stepWorkflow"));
    assertEquals(3, allrows.size());

    // 2 from BSI
    var brows =
        dbos.listWorkflows(
            new ListWorkflowsInput()
                .withWorkflowName("stepWorkflow")
                .withClassName("dev.dbos.transact.invocation.BearServiceImpl"));
    assertEquals(2, brows.size());

    // 2 from BSI
    var browsjust1 =
        dbos.listWorkflows(
            new ListWorkflowsInput()
                .withWorkflowName("stepWorkflow")
                .withClassName("dev.dbos.transact.invocation.BearServiceImpl")
                .withInstanceName("1"));
    assertEquals(1, browsjust1.size());
  }

  @Test
  public void enqueueForSpecificInstance() throws Exception {
    try (var client = pgContainer.dbosClient()) {
      var options =
          new dev.dbos.transact.DBOSClient.EnqueueOptions(
                  "stepWorkflow", "dev.dbos.transact.invocation.BearServiceImpl", "testQueue")
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

      var stat = client.findWorkflowStatus(handle.workflowId());
      assertEquals(
          "SUCCESS",
          stat.orElseThrow(() -> new AssertionError("Workflow status not found")).status());

      DBUtils.setWorkflowState(dataSource, handle.workflowId(), WorkflowState.PENDING.name());

      stat = client.findWorkflowStatus(handle.workflowId());
      assertEquals(
          "PENDING",
          stat.orElseThrow(() -> new AssertionError("Workflow status not found")).status());

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
      var eh = dbosExecutor.executeWorkflowById(handle.workflowId(), false, true);
      eh.getResult();
      stat = client.findWorkflowStatus(handle.workflowId());
      assertEquals(
          "SUCCESS",
          stat.orElseThrow(() -> new AssertionError("Workflow status not found")).status());
      assertEquals(0, bimpl1.nWfCalls);
      assertEquals(2, bimpla.nWfCalls);
    }
  }

  @Test
  void listSteps() throws Exception {
    var bh = dbos.startWorkflow(() -> bproxya.stepWorkflow());
    bh.getResult();
    var sh = dbos.startWorkflow(() -> bproxya.listSteps(bh.workflowId()));
    var ss = sh.getResult();
    assertEquals("1 1", ss);

    var steps = dbos.listWorkflowSteps(sh.workflowId());
    assertEquals(2, steps.size());
    assertEquals("DBOS.listWorkflows", steps.get(0).functionName());
    assertEquals("DBOS.listWorkflowSteps", steps.get(1).functionName());
  }
}
