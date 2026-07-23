package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExecutorIdRestampTest {

  static final String EXECUTOR_ID = "restamp-test-executor";

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose HikariDataSource dataSource;

  interface RestampService {
    String twoStepWorkflow();

    String stepOne();

    String stepTwo();
  }

  static class RestampServiceImpl implements RestampService {
    static CountDownLatch midpoint = new CountDownLatch(1);
    static CountDownLatch proceed = new CountDownLatch(1);

    private RestampService self;

    void setSelf(RestampService self) {
      this.self = self;
    }

    @Override
    @Workflow(name = "twoStepWorkflow")
    public String twoStepWorkflow() {
      var one = self.stepOne();
      midpoint.countDown();
      try {
        proceed.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      return one + self.stepTwo();
    }

    @Override
    @Step(name = "stepOne")
    public String stepOne() {
      return "one";
    }

    @Override
    @Step(name = "stepTwo")
    public String stepTwo() {
      return "two";
    }
  }

  @BeforeEach
  void setUp() {
    dbosConfig = pgContainer.dbosConfig().withExecutorId(EXECUTOR_ID);
    dataSource = pgContainer.dataSource();
    RestampServiceImpl.midpoint = new CountDownLatch(1);
    RestampServiceImpl.proceed = new CountDownLatch(1);
  }

  @Test
  void restampExecutorIdOnStepCheckpoint() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var impl = new RestampServiceImpl();
      var service = dbos.registerProxy(RestampService.class, impl);
      impl.setSelf(service);
      dbos.launch();

      String wfid = "restamp-wf-1";
      var handle = dbos.startWorkflow(service::twoStepWorkflow, new StartWorkflowOptions(wfid));

      assertTrue(RestampServiceImpl.midpoint.await(30, TimeUnit.SECONDS));
      setExecutorId(dataSource, wfid, "stale-executor");
      assertEquals("stale-executor", getExecutorId(dataSource, wfid));
      RestampServiceImpl.proceed.countDown();

      assertEquals("onetwo", handle.getResult());
      assertEquals(EXECUTOR_ID, getExecutorId(dataSource, wfid));
    }
  }

  private static void setExecutorId(DataSource ds, String workflowId, String executorId)
      throws SQLException {
    String sql = "UPDATE dbos.workflow_status SET executor_id = ? WHERE workflow_uuid = ?";
    try (Connection conn = ds.getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, executorId);
      pstmt.setString(2, workflowId);
      assertEquals(1, pstmt.executeUpdate());
    }
  }

  private static String getExecutorId(DataSource ds, String workflowId) throws SQLException {
    String sql = "SELECT executor_id FROM dbos.workflow_status WHERE workflow_uuid = ?";
    try (Connection conn = ds.getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, workflowId);
      try (var rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        return rs.getString("executor_id");
      }
    }
  }
}
