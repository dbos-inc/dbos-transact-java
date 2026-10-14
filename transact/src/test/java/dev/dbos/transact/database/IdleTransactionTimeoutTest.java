package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.dao.WorkflowDAO;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The system database connections DBOS creates end a transaction left idle too long, so a process
 * that stalls holding a workflow's row lock cannot block that workflow's cancel, resume or recovery
 * forever.
 */
public class IdleTransactionTimeoutTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);
  }

  private static String initSql(SystemDatabase sysdb) {
    return DBTestAccess.findHikariConfig(sysdb).orElseThrow().getConnectionInitSql();
  }

  @Test
  public void theDefaultIsOneMinute() {
    try (var sysdb = SystemDatabase.create(dbosConfig, null, dbosConfig.appName())) {
      assertEquals("SET idle_in_transaction_session_timeout = 60000", initSql(sysdb));
    }
  }

  @Test
  public void aConfiguredTimeoutIsApplied() {
    var config = dbosConfig.withSystemDatabaseIdleTransactionTimeout(Duration.ofMillis(250));
    try (var sysdb = SystemDatabase.create(config, null, config.appName())) {
      assertEquals("SET idle_in_transaction_session_timeout = 250", initSql(sysdb));
    }
  }

  @Test
  public void zeroLeavesTheServersSetting() {
    var config = dbosConfig.withSystemDatabaseIdleTransactionTimeout(Duration.ZERO);
    try (var sysdb = SystemDatabase.create(config, null, config.appName())) {
      assertNull(initSql(sysdb));
    }
  }

  @Test
  public void aUrlThatSetsItTakesPrecedence() {
    var url =
        dbosConfig.databaseUrl()
            + (dbosConfig.databaseUrl().contains("?") ? "&" : "?")
            + "options=-c%20idle_in_transaction_session_timeout%3D5000";
    var config = dbosConfig.withDatabaseUrl(url);
    try (var sysdb = SystemDatabase.create(config, null, config.appName())) {
      assertNull(initSql(sysdb));
    }
  }

  @Test
  public void aSuppliedDataSourceIsLeftAlone() {
    // A pool the application built itself, with no session settings.
    var hikari = new HikariConfig();
    hikari.setJdbcUrl(pgContainer.jdbcUrl());
    hikari.setUsername(pgContainer.username());
    hikari.setPassword(pgContainer.password());
    try (var dataSource = new HikariDataSource(hikari);
        var sysdb = new SystemDatabase(dataSource, "dbos", null, false, null)) {
      assertNull(initSql(sysdb));
    }
  }

  @Test
  public void aTimeoutLargerThanPostgresTakesIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbosConfig.withSystemDatabaseIdleTransactionTimeout(
                Duration.ofMillis(Integer.MAX_VALUE + 1L)));
  }

  // A session that took a workflow's owner lock and then stalled is ended by the server, which
  // releases the lock: a cancel waiting on it goes through.
  @Test
  public void aStrandedOwnerLockIsReleased() throws Exception {
    var config = dbosConfig.withSystemDatabaseIdleTransactionTimeout(Duration.ofMillis(500));
    try (var sysdb = SystemDatabase.create(config, null, config.appName())) {
      var workflowId = "idle-stranded-%d".formatted(System.nanoTime());
      var owner =
          sysdb
              .initWorkflowStatus(WorkflowStatusInternalBuilder.create(workflowId).build(), null)
              .ownerXid();

      var ctx = sysdb.context();
      try (var stranded = ctx.getConnection()) {
        stranded.setAutoCommit(false);
        WorkflowDAO.checkOwner(stranded, ctx.schema(), workflowId, owner);

        // The stalled session holds the row; the cancel waits for the server to end it.
        var cancel =
            CompletableFuture.runAsync(() -> sysdb.cancelWorkflows(List.of(workflowId), false));
        cancel.get(30, TimeUnit.SECONDS);

        assertNull(WorkflowDAO.getWorkflowOwner(ctx, workflowId), "the cancel released it");
        assertThrows(SQLException.class, stranded::commit, "the stalled session was ended");
      }
    }
  }
}
