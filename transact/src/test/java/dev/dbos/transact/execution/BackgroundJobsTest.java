package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.AdvisoryLocks;
import dev.dbos.transact.database.BackgroundJobLock;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import javax.sql.DataSource;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BackgroundJobsTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose SystemDatabase local;
  @AutoClose SystemDatabase peer;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
  }

  private void openSystemDatabases() {
    MigrationManager.runMigrations(dbosConfig);
    local = SystemDatabase.create(dbosConfig, "local", dbosConfig.appName());
    peer = SystemDatabase.create(dbosConfig, "peer", dbosConfig.appName());
  }

  private static void waitUntil(BooleanSupplier condition, Duration timeout, String what)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      assertTrue(System.nanoTime() - deadline < 0, what);
      Thread.sleep(50);
    }
  }

  private String schema() {
    return SystemDatabase.sanitizeSchema(dbosConfig.databaseSchema());
  }

  @Test
  void anErrorTakingTheLockReturnsItsConnection() throws Exception {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB takes no connection");
    openSystemDatabases();
    try (var pool = pgContainer.dataSource()) {
      var failing = new AtomicBoolean();
      var source =
          (DataSource)
              Proxy.newProxyInstance(
                  BackgroundJobsTest.class.getClassLoader(),
                  new Class<?>[] {DataSource.class},
                  (proxy, method, args) -> {
                    var result = method.invoke(pool, args);
                    return failing.get() && result instanceof Connection c
                        ? failingStatements(c)
                        : result;
                  });
      try (var sysdb =
          SystemDatabase.create(
              dbosConfig.withDataSource(source), "failing", dbosConfig.appName())) {
        failing.set(true);
        assertThrows(
            NoClassDefFoundError.class,
            () -> sysdb.backgroundJobLock("leak_job", BackgroundJobLock.DEFAULT_IDLE_TIMEOUT));
      }
      assertEquals(0, pool.getHikariPoolMXBean().getActiveConnections());
    }
  }

  private static Connection failingStatements(Connection target) {
    return (Connection)
        Proxy.newProxyInstance(
            BackgroundJobsTest.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (method.getName().equals("createStatement")) {
                throw new NoClassDefFoundError("injected");
              }
              return method.invoke(target, args);
            });
  }

  @Test
  void theBackgroundJobLockSessionHoldsNoSnapshot() throws Exception {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB has no advisory locks");
    openSystemDatabases();
    var key = AdvisoryLocks.backgroundJobKey(schema(), dbosConfig.appName(), "xmin_job");
    try (var lock = local.backgroundJobLock("xmin_job", BackgroundJobLock.DEFAULT_IDLE_TIMEOUT)) {
      assertTrue(lock.acquired());
      // Past the driver's prepare threshold.
      for (int i = 0; i < 8; i++) {
        lock.renew();
      }
      try (var conn =
              DriverManager.getConnection(
                  pgContainer.jdbcUrl(), pgContainer.username(), pgContainer.password());
          var stmt =
              conn.prepareStatement(
                  "SELECT a.backend_xmin::text, a.state FROM pg_stat_activity a"
                      + " JOIN pg_locks l ON l.pid = a.pid WHERE l.locktype = 'advisory'"
                      + " AND l.granted AND l.classid::int8 = ? AND l.objid::int8 = ?"
                      + " AND l.objsubid = 1")) {
        stmt.setLong(1, (key >> 32) & 0xFFFFFFFFL);
        stmt.setLong(2, key & 0xFFFFFFFFL);
        try (var rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "the lock is not held");
          assertNull(rs.getString(1), "the lock's idle session holds a snapshot");
          assertEquals("idle in transaction", rs.getString(2));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  void concurrentIndexBuildsDoNotWaitOnTheBackgroundJobLock() throws Exception {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB has no advisory locks");
    openSystemDatabases();
    try (var conn =
            DriverManager.getConnection(
                pgContainer.jdbcUrl(), pgContainer.username(), pgContainer.password());
        var stmt = conn.createStatement()) {
      stmt.execute("SET statement_timeout = '10s'");
      stmt.execute("DROP TABLE IF EXISTS cic_probe");
      stmt.execute("CREATE TABLE cic_probe (id int)");
      try (var lock = local.backgroundJobLock("cic_job", BackgroundJobLock.DEFAULT_IDLE_TIMEOUT)) {
        assertTrue(lock.acquired());
        for (int i = 0; i < 8; i++) {
          lock.renew();
        }
        long started = System.nanoTime();
        stmt.execute("CREATE INDEX CONCURRENTLY cic_probe_idx ON cic_probe (id)");
        assertTrue(
            Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(3)) < 0);
      }
      stmt.execute("DROP TABLE cic_probe");
    }
  }

  @Test
  void aDriverThatCannotBeUnwrappedStillLocksAndRenews() throws Exception {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB grants every lock");
    openSystemDatabases();
    var timeout = BackgroundJobLock.DEFAULT_IDLE_TIMEOUT;
    try (var pool = pgContainer.dataSource();
        var opaque =
            SystemDatabase.create(
                dbosConfig.withDataSource(hidingTheDriver(pool)), "opaque", dbosConfig.appName());
        var lock = opaque.backgroundJobLock("opaque_job", timeout)) {
      assertTrue(lock.acquired());
      lock.renew();
      try (var refused = peer.backgroundJobLock("opaque_job", timeout)) {
        assertFalse(refused.acquired());
      }
    }
  }

  // The data source, with statements that cannot be unwrapped to the driver's.
  private static DataSource hidingTheDriver(DataSource target) {
    return (DataSource) hiding(target, DataSource.class);
  }

  private static Object hiding(Object target, Class<?> type) {
    return Proxy.newProxyInstance(
        BackgroundJobsTest.class.getClassLoader(),
        new Class<?>[] {type},
        (proxy, method, args) -> {
          if (method.getName().equals("isWrapperFor")) {
            return false;
          }
          Object result;
          try {
            result = method.invoke(target, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
          return switch (method.getName()) {
            case "getConnection" -> hiding(result, Connection.class);
            case "createStatement" -> hiding(result, Statement.class);
            default -> result;
          };
        });
  }

  @Test
  void aReleasedLockIsFreeForAPeer() throws Exception {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB grants every lock");
    openSystemDatabases();
    var timeout = BackgroundJobLock.DEFAULT_IDLE_TIMEOUT;
    try (var lock = local.backgroundJobLock("release_job", timeout)) {
      assertTrue(lock.acquired());
      try (var refused = peer.backgroundJobLock("release_job", timeout)) {
        assertFalse(refused.acquired());
      }
    }
    try (var lock = peer.backgroundJobLock("release_job", timeout)) {
      assertTrue(lock.acquired());
    }
  }

  @Test
  void cockroachGrantsEveryProcessTheLock() throws Exception {
    Assumptions.assumeTrue(PgContainer.USE_COCKROACH_DB, "CockroachDB only");
    openSystemDatabases();
    var timeout = BackgroundJobLock.DEFAULT_IDLE_TIMEOUT;
    try (var first = local.backgroundJobLock("crdb_job", timeout);
        var second = peer.backgroundJobLock("crdb_job", timeout)) {
      assertTrue(first.acquired());
      assertTrue(second.acquired());
      first.renew();
    }
  }

}
