package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.sql.SQLException;
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
  BearServiceImpl bimpl;
  private HawkService hproxy;
  private BearService bproxy;
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
    bimpl = new BearServiceImpl();

    hproxy =
        dbos.<HawkService>Workflow()
            .interfaceClass(HawkService.class)
            .implementation(himpl)
            .build();
    himpl.setProxy(hproxy);
    bproxy =
        dbos.<BearService>Workflow()
            .interfaceClass(BearService.class)
            .implementation(bimpl)
            .build();
    bimpl.setProxy(bproxy);

    dbos.launch();
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dbos.shutdown();
  }

  @Test
  void startWorkflow() throws Exception {
    var bhandle =
        dbos.startWorkflow(
            () -> {
              return bproxy.stepWorkflow();
            });
    var bresult = bhandle.getResult();
    assertEquals(
        localDate,
        bresult
            .atZone(ZoneId.systemDefault()) // or another zone
            .toLocalDate()
            .format(DateTimeFormatter.ISO_DATE));

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
    assertEquals(1, bimpl.nWfCalls);

    var brows =
        dbos.listWorkflows(
            new ListWorkflowsInput.Builder().workflowID(bhandle.getWorkflowId()).build());
    assertEquals(1, brows.size());
    var brow = brows.get(0);
    assertEquals(bhandle.getWorkflowId(), brow.workflowId());
    assertEquals("stepWorkflow", brow.name());
    assertEquals("dev.dbos.transact.invocation.BearServiceImpl", brow.className());
    assertEquals("SUCCESS", brow.status());

    var hrows =
        dbos.listWorkflows(
            new ListWorkflowsInput.Builder().workflowID(hhandle.getWorkflowId()).build());
    assertEquals(1, hrows.size());
    var hrow = hrows.get(0);
    assertEquals(hhandle.getWorkflowId(), hrow.workflowId());
    assertEquals("stepWorkflow", hrow.name());
    assertEquals("dev.dbos.transact.invocation.HawkServiceImpl", hrow.className());
    assertEquals("SUCCESS", hrow.status());
  }
}
