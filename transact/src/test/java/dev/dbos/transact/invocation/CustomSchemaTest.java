package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class CustomSchemaTest {
  private static DBOSConfig dbosConfig;
  private static final String schema = "C$+0m'";
  private HawkService proxy;
  private HikariDataSource dataSource;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  @BeforeAll
  static void onetimeSetup() throws Exception {

    dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withDatabaseSchema(schema)
            .withMaximumPoolSize(2);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
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
    validateWorkflow();
  }

  @Test
  void startWorkflow() throws Exception {
    var handle =
        DBOS.startWorkflow(
            () -> {
              return proxy.simpleWorkflow();
            });
    var result = handle.getResult();
    assertEquals(localDate, result);
    validateWorkflow();
  }

  void validateWorkflow() throws SQLException {
    var rows = DBUtils.getWorkflowRows(dataSource, schema);
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
}
