package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.Connection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.*;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
class SystemDatabaseTest {

  private static SystemDatabase systemDatabase;
  private static DBOSConfig dbosConfig;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    SystemDatabaseTest.dbosConfig =
        new DBOSConfig.Builder()
            .appName("systemdbtest")
            .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .maximumPoolSize(3)
            .build();

    DBUtils.recreateDB(dbosConfig);
    MigrationManager.runMigrations(dbosConfig);
    systemDatabase = new SystemDatabase(dbosConfig);
  }

  @AfterAll
  static void onetimeTearDown() throws Exception {
    systemDatabase.close();
  }

  @BeforeEach
  void setUp() {}

  @AfterEach
  void tearDown() {}

  @Test
  void insertWorkflowStatusSuccess() throws Exception {

    String workflowId = UUID.randomUUID().toString();

    WorkflowStatusInternal wfStatusInternal =
        new WorkflowStatusInternal(
            workflowId,
            WorkflowState.SUCCESS,
            "OrderProcessingWorkflow",
            "com.example.workflows.OrderWorkflow",
            "prod-config",
            "user123@example.com",
            "admin",
            "admin,operator",
            "{\"result\":\"success\"}",
            null,
            System.currentTimeMillis() - 3600000,
            System.currentTimeMillis(),
            "order-queue",
            "exec-98765",
            "1.2.3",
            "order-app-123",
            0,
            300000l,
            System.currentTimeMillis() + 2400000,
            "dedup-112233",
            1,
            "{\"orderId\":\"ORD-12345\"}");

    try (Connection conn = systemDatabase.getSysDBConnection()) {
      var result = systemDatabase.initWorkflowStatus(wfStatusInternal, null);
      var nstat = systemDatabase.getWorkflowStatus(workflowId);

      assertNotNull(result);
      assertEquals(wfStatusInternal.getStatus().toString(), result.getStatus());
      assertEquals(wfStatusInternal.getName(), nstat.name());
      assertEquals(wfStatusInternal.getClassName(), nstat.className());
      assertEquals(wfStatusInternal.getInstanceName(), nstat.instanceName());
      assertEquals(wfStatusInternal.getQueueName(), nstat.queueName());
      assertEquals(wfStatusInternal.getWorkflowDeadlineEpochMs(), nstat.deadlineEpochMs());
    }
  }
}
