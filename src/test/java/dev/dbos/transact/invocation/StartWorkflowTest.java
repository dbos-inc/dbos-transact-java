package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class StartWorkflowTest {
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
  void startWorkflow() throws Exception {
    var handle =
        dbos.startWorkflow(
            () -> {
              return proxy.simpleWorkflow();
            });
    var result = handle.getResult();
    assertEquals(localDate, result);

    var rows = dbos.listWorkflows(null);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(handle.getWorkflowId(), row.workflowId());
    assertEquals("SUCCESS", row.status());
  }

  @Test
  void startWorkflowWithWorkflowId() throws Exception {

    String workflowId = "startWorkflowWithWorkflowId";
    var options = new StartWorkflowOptions(workflowId);
    var handle = dbos.startWorkflow(() -> proxy.simpleWorkflow(), options);
    assertEquals(workflowId, handle.getWorkflowId());
    var result = handle.getResult();
    assertEquals(localDate, result);

    var row = handle.getStatus();
    assertNotNull(row);
    assertEquals(workflowId, row.workflowId());
    assertEquals("SUCCESS", row.status());
    assertNull(row.getTimeout());
    assertNull(row.getDeadline());
  }

  @Test
  void startWorkflowWithTimeout() throws Exception {

    String workflowId = "startWorkflowWithTimeout";
    var options = new StartWorkflowOptions(workflowId).withTimeout(1, TimeUnit.SECONDS);
    var handle = dbos.startWorkflow(() -> proxy.simpleWorkflow(), options);
    assertEquals(workflowId, handle.getWorkflowId());
    var result = handle.getResult();
    assertEquals(localDate, result);

    var row = dbos.retrieveWorkflow(workflowId);
    assertNotNull(row);
    assertEquals(workflowId, row.getWorkflowId());
    assertEquals("SUCCESS", row.getStatus().status());
    assertEquals(1000, row.getStatus().workflowTimeoutMs());
    assertNotNull(row.getStatus().workflowDeadlineEpochMs());
  }
}
