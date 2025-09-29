package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.sql.SQLException;
import java.time.LocalDate;
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
  private HawkService hproxy;
  private BearService bproxy;
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
    var himpl = new HawkServiceImpl();
    var bimpl = new BearServiceImpl();

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
    assertEquals(localDate, LocalDate.from(bresult).format(DateTimeFormatter.ISO_DATE));

    var hhandle =
        dbos.startWorkflow(
            () -> {
              return hproxy.stepWorkflow();
            });
    var hresult = hhandle.getResult();
    assertEquals(localDate, LocalDate.from(hresult).format(DateTimeFormatter.ISO_DATE));

    var brows =
        dbos.listWorkflows(
            new ListWorkflowsInput.Builder().workflowID(bhandle.getWorkflowId()).build());
    assertEquals(1, brows.size());
    var brow = brows.get(0);
    assertEquals(hhandle.getWorkflowId(), brow.workflowId());
    assertEquals("startWorkflow", brow.name());
    assertEquals("BearService", brow.className());
    assertEquals("SUCCESS", brow.status());

    var hrows =
        dbos.listWorkflows(
            new ListWorkflowsInput.Builder().workflowID(hhandle.getWorkflowId()).build());
    assertEquals(1, hrows.size());
    var hrow = hrows.get(0);
    assertEquals(hhandle.getWorkflowId(), hrow.workflowId());
    assertEquals("startWorkflow", hrow.name());
    assertEquals("HawkService", brow.className());
    assertEquals("SUCCESS", hrow.status());
  }
}
