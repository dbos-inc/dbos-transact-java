package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils.DBSettings;

import java.sql.SQLException;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class LifecycleTest {
  private static LifecycleTestWorkflowsImpl impl;
  private static TestLifecycleService svc;

  private static final DBSettings db = DBSettings.get();
  private DBOSConfig dbosConfig;
  private HikariDataSource dataSource;

  @BeforeEach
  void beforeEachTest() throws SQLException {
    db.recreate();

    dataSource = db.dataSource();
    dbosConfig = DBOSConfig.defaults("systemdbtest").withDataSource(dataSource);

    DBOS.reinitialize(dbosConfig);

    impl = new LifecycleTestWorkflowsImpl();
    DBOS.registerWorkflows(LifecycleTestWorkflows.class, impl, "inst1");
    svc = new TestLifecycleService();
    DBOS.registerLifecycleListener(svc);
    DBOS.registerWorkflows(LifecycleTestWorkflows.class, new LifecycleTestWorkflowsImpl(), "instA");

    assertEquals(0, svc.launchCount);
    DBOS.launch();
    assertEquals(1, svc.launchCount);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
    dataSource.close();
  }

  @Test
  void checkThatItAllHappened() throws Exception {
    // Pretend this is an external event
    var total = svc.runThemAll();
    assertEquals(3, impl.nInstances); // One of these is internal... for better or worse
    assertEquals(4, impl.nWfs);
    assertEquals(14, svc.annotationCount);
    assertEquals(30, total);

    assertEquals(0, svc.shutdownCount);
    DBOS.shutdown();
    assertEquals(1, svc.shutdownCount);
  }

  @Test
  void deactivateLifecycleListeners() throws Exception {
    // Pretend this is an external event
    var total = svc.runThemAll();
    assertEquals(3, impl.nInstances); // One of these is internal... for better or worse
    assertEquals(4, impl.nWfs);
    assertEquals(14, svc.annotationCount);
    assertEquals(30, total);

    assertEquals(0, svc.shutdownCount);
    DBOSTestAccess.getDbosExecutor().deactivateLifecycleListeners();
    assertEquals(1, svc.shutdownCount);
    DBOS.shutdown();
    assertEquals(2, svc.shutdownCount);
  }
}
