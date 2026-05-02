package dev.dbos.transact.txstep;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface FactoryTestService {
  int greet(String user) throws SQLException;
}

class FactoryTestServiceImpl implements FactoryTestService {

  private final DBOS dbos;
  private final JdbcStepFactory stepFactory;

  public FactoryTestServiceImpl(DBOS dbos, JdbcStepFactory stepFactory) {
    this.dbos = dbos;
    this.stepFactory = stepFactory;
  }

  int insertGreeting(Connection conn, String user) throws SQLException {
    var sql =
        """
            INSERT INTO greetings(name, greet_count)
            VALUES (?, 1)
            ON CONFLICT(name)
            DO UPDATE SET greet_count = greetings.greet_count + 1
            RETURNING greet_count
            """;

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(user));
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt("greet_count");
        } else {
          return 0;
        }
      }
    }
  }

  @Override
  @Workflow
  public int greet(String user) throws SQLException {
    return stepFactory.txStep((Connection c) -> insertGreeting(c, user), "insertGreeting");
  }
}

public class JdbcStepFactoryTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  JdbcStepFactory stepFactory;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dataSource = pgContainer.dataSource();
    dbos = new DBOS(dbosConfig);
    stepFactory = new JdbcStepFactory(dbos, dataSource);
  }

  @Test
  public void foo() throws Exception {
    {
      var sql =
          "CREATE TABLE greetings(name text NOT NULL, greet_count integer DEFAULT 0, PRIMARY KEY(name))";
      try (var conn = dataSource.getConnection();
          var stmt = conn.createStatement()) {
        stmt.execute(sql);
      }
    }
    var proxy =
        dbos.registerProxy(FactoryTestService.class, new FactoryTestServiceImpl(dbos, stepFactory));

    dbos.launch();
    assertEquals(1, proxy.greet("Luna"));
    assertEquals(2, proxy.greet("Luna"));
    assertEquals(3, proxy.greet("Luna"));
    assertEquals(4, proxy.greet("Luna"));
    assertEquals(5, proxy.greet("Luna"));

    var txStepRows = DBUtils.getTxStepRows(dataSource);
    assertEquals(5, txStepRows.size());

  }
}
