package dev.dbos.transact.jooq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;

import com.zaxxer.hikari.HikariDataSource;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A workflow deletes its Jooq checkpoints when its execution finishes. */
public class JooqCompletedStepCheckpointsTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  FactoryTestService proxy;

  @BeforeEach
  void beforeEach() throws SQLException {
    var dbosConfig = pgContainer.dbosConfig();
    dataSource = pgContainer.dataSource();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE greetings(name text NOT NULL, greet_count integer DEFAULT 0, PRIMARY KEY(name))");
    }

    dbos = new DBOS(dbosConfig);
    var factory = new JooqStepFactory(dbos, DSL.using(dataSource, SQLDialect.POSTGRES));
    proxy =
        dbos.registerProxy(
            FactoryTestService.class,
            new FactoryTestServiceImpl(
                factory, dataSource, SystemDatabase.sanitizeSchema(dbosConfig.databaseSchema())));
    dbos.launch();
  }

  @Test
  public void aWorkflowThatReturnsDeletesItsCheckpoints() throws Exception {
    try (var _o = new WorkflowOptions("wf-returns").setContext()) {
      assertEquals(1, proxy.insertThenReadWorkflow("returnsUser").greetCount());
    }
    assertEquals(0, DBUtils.getTxStepRows(dataSource, "wf-returns").size());
  }

  @Test
  public void aWorkflowThatThrowsDeletesItsCheckpoints() throws Exception {
    try (var _o = new WorkflowOptions("wf-throws").setContext()) {
      assertThrows(RuntimeException.class, () -> proxy.errorWorkflow("throwsUser"));
    }
    assertEquals(0, DBUtils.getTxStepRows(dataSource, "wf-throws").size());
  }
}
