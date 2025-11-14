package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LifecycleTest {
  private static DBOSConfig dbosConfig;
  private static LifecycleTestWorkflowsImpl impl;
  private static TestLifecycleService svc;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    dbosConfig =
        DBOSConfig.defaultsFromEnv("lifecycletest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withMaximumPoolSize(2);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
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
    assertEquals(0, svc.shutdownCount);
    DBOS.shutdown();
    assertEquals(1, svc.shutdownCount);
  }

  @Test
  void checkThatItAllHappened() throws Exception {
    // Pretend this is an external event
    var total = svc.runThemAll();
    assertEquals(3, impl.nInstances); // One of these is internal... for better or worse
    assertEquals(4, impl.nWfs);
    assertEquals(14, svc.annotationCount);
    assertEquals(30, total);
  }
}
