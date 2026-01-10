package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.DBUtils.DBSettings;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class CustomSchemaTest {
  private static final String schema = "C$+0m'";
  private HawkService proxy;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
  private static final DBSettings db = DBSettings.get();
  private DBOSConfig dbosConfig;
  private HikariDataSource dataSource;

  @BeforeEach
  void beforeEachTest() throws SQLException {
    db.recreate();

    dataSource = db.dataSource();
    dbosConfig =
        DBOSConfig.defaults("systemdbtest").withDataSource(dataSource).withDatabaseSchema(schema);

    DBOS.reinitialize(dbosConfig);

    var impl = new HawkServiceImpl();
    proxy = DBOS.registerWorkflows(HawkService.class, impl);
    impl.setProxy(proxy);

    DBOS.launch();

    dataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
    dataSource.close();
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
