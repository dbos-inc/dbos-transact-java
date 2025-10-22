package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
public class SystemDatabaseTest {
  private static DBOSConfig config;
  private SystemDatabase sysdb;
  private HikariDataSource dataSource;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    config =
        new DBOSConfig.Builder()
            .appName("systemdbtest")
            .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .maximumPoolSize(3)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(config);
    MigrationManager.runMigrations(config);
    sysdb = new SystemDatabase(config);
    dataSource = SystemDatabase.createDataSource(config);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dataSource.close();
    sysdb.close();
  }

  @Test
  public void testRetries() throws Exception {
    var wfid = "wfid-1";
    var status =
        new WorkflowStatusInternal(
            wfid,
            WorkflowState.PENDING,
            "wf-name",
            null, 
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            1,
            null,
            null,
            null,
            0,
            "wf-input");

    for (var i = 1; i <= 6; i++) {
      var result1 = sysdb.initWorkflowStatus(status, 5);
      assertEquals(WorkflowState.PENDING.toString(), result1.getStatus());
      assertEquals(wfid, result1.getWorkflowId());
      assertEquals(0, result1.getDeadlineEpochMS());

      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertNotNull(row);
      assertEquals(i, row.recoveryAttempts());
    }

    assertThrows(DBOSMaxRecoveryAttemptsExceededException.class, () -> sysdb.initWorkflowStatus(status, 5));
  }
}
