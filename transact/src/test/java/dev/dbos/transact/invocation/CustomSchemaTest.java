package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class CustomSchemaTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();
  private static final String schema = "F8nny_sCHem@-n@m3";
  @AutoClose DBOS dbos;
  private HawkService proxy;
  @AutoClose HikariDataSource dataSource;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  @BeforeEach
  void beforeEachTest() throws SQLException {
    var dbosConfig = pgContainer.dbosConfig().withDatabaseSchema(schema);
    dbos = new DBOS(dbosConfig);
    var impl = new HawkServiceImpl(dbos);
    proxy = dbos.registerWorkflows(HawkService.class, impl);
    impl.setProxy(proxy);

    dbos.launch();

    dataSource = pgContainer.dataSource();
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
        dbos.startWorkflow(
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
    assertEquals("simpleWorkflow", row.workflowName());
    assertEquals("dev.dbos.transact.invocation.HawkServiceImpl", row.className());
    assertNotNull(row.output());
    assertNull(row.error());
    assertNull(row.timeoutMs());
    assertNull(row.deadlineEpochMs());
  }
}
