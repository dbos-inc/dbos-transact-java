package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.txstep.PostgresStepFactory;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface OwnershipCheckService {
  boolean checkWithoutAConnection();
}

class OwnershipCheckServiceImpl implements OwnershipCheckService {
  private final DBOS dbos;

  OwnershipCheckServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public boolean checkWithoutAConnection() {
    // The ownership check a step factory makes after its transaction rolled back: no connection.
    return PostgresStepFactory.mayRecordFailure(dbos, DBOS.workflowId());
  }
}

/**
 * SystemDatabase.reachesThisDatabase: whether a step's connection reaches the system database, as a
 * user that may lock its workflow rows, so ownership can be checked on that connection.
 */
class ReachesThisDatabaseTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource pool;
  @AutoClose DBOS dbos;
  SystemDatabase sysdb;
  OwnershipCheckService proxy;

  @BeforeEach
  void beforeEach() {
    pool = pgContainer.dataSource();
    dbos = new DBOS(pgContainer.dbosConfig().withDataSource(pool));
    proxy = dbos.registerProxy(OwnershipCheckService.class, new OwnershipCheckServiceImpl(dbos));
    dbos.launch();
    sysdb = DBOSTestAccess.getSystemDatabase(dbos);
  }

  private static void assumePostgres() {
    Assumptions.assumeFalse(
        PgContainer.USE_COCKROACH_DB, "PG-only: CockroachDB cannot identify its database");
  }

  // A pool of one connection, so every probe through it is of the same physical connection.
  private static HikariDataSource singleConnection(String url, String user, String password) {
    var config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setUsername(user);
    config.setPassword(password);
    config.setMaximumPoolSize(1);
    return new HikariDataSource(config);
  }

  private HikariDataSource singleConnection() {
    return singleConnection(pgContainer.jdbcUrl(), pgContainer.username(), pgContainer.password());
  }

  // Probes a connection inside an open transaction, as a step's is, and checks the transaction is
  // still usable afterwards.
  private boolean probe(DataSource dataSource) throws SQLException {
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        var reaches = sysdb.reachesThisDatabase(conn);
        try (var stmt = conn.createStatement()) {
          stmt.execute("SELECT 1");
        }
        return reaches;
      } finally {
        conn.rollback();
      }
    }
  }

  private int probes() {
    return DBTestAccess.sameDatabaseProbes(sysdb);
  }

  private void execute(String sql) throws SQLException {
    execute(pgContainer.jdbcUrl(), sql);
  }

  private void execute(String url, String sql) throws SQLException {
    try (var ds = singleConnection(url, pgContainer.username(), pgContainer.password());
        var conn = ds.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  private static String unique(String prefix) {
    return prefix + UUID.randomUUID().toString().replace("-", "");
  }

  private String urlOf(String database) {
    return pgContainer.jdbcUrl().replaceFirst("/[^/]+$", "/" + database);
  }

  @Test
  void onCockroachDbItAnswersNoAndLeavesTheTransactionUsable() throws Exception {
    Assumptions.assumeTrue(PgContainer.USE_COCKROACH_DB, "CockroachDB only");
    // CockroachDB has neither pg_control_system() nor a server start time to identify the
    // database by, so steps there check ownership on a second connection, as before.
    assertFalse(probe(pool));
  }

  @Test
  void aConnectionFromTheSystemDatabasesPool() throws Exception {
    assumePostgres();
    assertTrue(probe(pool));
  }

  @Test
  void aConnectionIsProbedOnce() throws Exception {
    assumePostgres();
    try (var one = singleConnection()) {
      assertTrue(probe(one));
      assertTrue(probe(one));
      assertTrue(probe(one));
    }
    assertEquals(1, probes());
  }

  @Test
  void aWrapperThatReturnsItselfStillFindsTheConnectionsAnswer() throws Exception {
    assumePostgres();
    try (var one = singleConnection()) {
      for (int i = 0; i < 3; i++) {
        try (var conn = one.getConnection()) {
          conn.setAutoCommit(false);
          try {
            assertTrue(sysdb.reachesThisDatabase(selfUnwrapping(conn)));
          } finally {
            conn.rollback();
          }
        }
      }
    }
    // A new wrapper each time, but the same driver connection beneath.
    assertEquals(1, probes());
  }

  // A wrapper, as some proxies are, that answers unwrap(Connection.class) with itself and
  // delegates everything else.
  private static Connection selfUnwrapping(Connection target) {
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (method.getName().equals("unwrap") && args[0] == Connection.class) {
                return proxy;
              }
              if (method.getName().equals("isWrapperFor") && args[0] == Connection.class) {
                return true;
              }
              try {
                return method.invoke(target, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  @Test
  void aConnectionFromAnotherPoolToTheSameDatabase() throws Exception {
    assumePostgres();
    try (var other = singleConnection()) {
      assertTrue(probe(other));
    }
  }

  @Test
  void aConnectionToAnotherDatabase() throws Exception {
    assumePostgres();
    var name = unique("reaches_other_");
    execute("CREATE DATABASE " + name);
    try {
      // A workflow table of its own, so only the database's identity tells them apart.
      execute(urlOf(name), "CREATE SCHEMA dbos");
      execute(urlOf(name), "CREATE TABLE dbos.workflow_status (workflow_uuid TEXT PRIMARY KEY)");
      try (var other =
          singleConnection(urlOf(name), pgContainer.username(), pgContainer.password())) {
        assertFalse(probe(other));
      }
    } finally {
      execute("DROP DATABASE " + name + " WITH (FORCE)");
    }
  }

  @Test
  void aUserWhoMayNotLockWorkflowRowsIsAnsweredNo() throws Exception {
    assumePostgres();
    var role = unique("reaches_role_");
    try {
      execute("CREATE ROLE " + role + " LOGIN PASSWORD 'pw'");
      // May see the table, but not lock its rows.
      execute("GRANT USAGE ON SCHEMA dbos TO " + role);
      execute("GRANT SELECT ON dbos.workflow_status TO " + role);
      try (var restricted = singleConnection(pgContainer.jdbcUrl(), role, "pw")) {
        assertFalse(probe(restricted));
        assertFalse(probe(restricted));
      }
      // A definite no is remembered for the connection.
      assertEquals(1, probes());
    } finally {
      execute("REVOKE ALL ON dbos.workflow_status FROM " + role);
      execute("REVOKE ALL ON SCHEMA dbos FROM " + role);
      execute("DROP ROLE IF EXISTS " + role);
    }
  }

  @Test
  void aProbeThatFailsAnswersNoAndIsTriedAgain() throws Exception {
    assumePostgres();
    var role = unique("reaches_role_");
    try {
      // Without USAGE on the schema, looking up the workflow table raises.
      execute("CREATE ROLE " + role + " LOGIN PASSWORD 'pw'");
      try (var restricted = singleConnection(pgContainer.jdbcUrl(), role, "pw")) {
        assertFalse(probe(restricted));
        assertFalse(probe(restricted));
      }
      // A failure is not remembered: the connection is probed again.
      assertEquals(2, probes());
    } finally {
      execute("DROP ROLE IF EXISTS " + role);
    }
  }

  @Test
  void aChangedIdentityIsProbedAgainAndARefreshRestoresIt() throws Exception {
    assumePostgres();
    var identity = sysdb.databaseIdentity();
    assertNotNull(identity);
    try (var one = singleConnection()) {
      assertTrue(probe(one));

      // As after a restart of the database server: the connection no longer matches.
      sysdb.replaceDatabaseIdentity(
          new SystemDatabase.DatabaseIdentity(identity.sql(), "restarted"));
      assertFalse(probe(one));

      // A refresh reads the real identity again, and the connection is probed again.
      sysdb.refreshDatabaseIdentity();
      assertEquals(identity, sysdb.databaseIdentity());
      assertTrue(probe(one));
    }
    assertEquals(3, probes());
  }

  @Test
  void aCheckWithoutAConnectionProbesNothing() {
    try (var _o = new WorkflowOptions("no-connection-check").setContext()) {
      assertTrue(proxy.checkWithoutAConnection());
    }
    assertEquals(0, probes());
  }

  @Test
  void identifiesTheServerBySystemIdentifierAndStartTimeOrElseByStartTime() throws Exception {
    assumePostgres();
    var name = unique("reaches_identity_");
    var role = unique("reaches_role_");
    execute("CREATE DATABASE " + name);
    try {
      execute("CREATE ROLE " + role + " LOGIN PASSWORD 'pw'");
      try (var admin =
              singleConnection(urlOf(name), pgContainer.username(), pgContainer.password());
          var user = singleConnection(urlOf(name), role, "pw")) {
        String systemIdentifier;
        String startTime;
        SystemDatabase.DatabaseIdentity full;
        try (var conn = admin.getConnection();
            var stmt = conn.createStatement();
            var rs =
                stmt.executeQuery(
                    "SELECT system_identifier, extract(epoch FROM pg_postmaster_start_time())"
                        + " FROM pg_control_system()")) {
          rs.next();
          systemIdentifier = rs.getString(1);
          startTime = rs.getString(2);
          full = SystemDatabase.readDatabaseIdentity(conn);
        }
        assertEquals(SystemDatabase.DATABASE_IDENTITY_SQL, full.sql());
        var parts = full.value().split("/");
        assertEquals(4, parts.length);
        assertEquals(systemIdentifier, parts[0]);
        assertEquals(startTime, parts[1]);
        assertEquals(name, parts[2]);

        // Any user may read it by default.
        try (var conn = user.getConnection()) {
          assertEquals(full, SystemDatabase.readDatabaseIdentity(conn));
        }

        // A user who may not falls back to the identity without system_identifier.
        execute(urlOf(name), "REVOKE EXECUTE ON FUNCTION pg_control_system() FROM PUBLIC");
        try (var conn = user.getConnection()) {
          var byStartTime = SystemDatabase.readDatabaseIdentity(conn);
          assertEquals(SystemDatabase.DATABASE_IDENTITY_BY_START_TIME_SQL, byStartTime.sql());
          assertEquals(String.join("/", parts[1], parts[2], parts[3]), byStartTime.value());
        }
      }
    } finally {
      execute("DROP DATABASE " + name + " WITH (FORCE)");
      execute("DROP ROLE IF EXISTS " + role);
    }
  }
}
