package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Workflow;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface TestLifecycleAnnotation {
  int count() default 4;
}

interface LifecycleTestWorkflows {
  public int runWf1(int nClasses, int nWfs);

  public int runWf2(int nClasses, int nWfs);

  public int doNotRunWF(int nClasses, int nWfs);
}

class LifecycleTestWorkflowsImpl implements LifecycleTestWorkflows {
  int nWfs = 0, nInstances = 0;

  @Workflow()
  @TestLifecycleAnnotation(count = 3)
  public int runWf1(int nInstances, int nWfs) {
    this.nInstances = nInstances;
    this.nWfs = nWfs;
    return 8;
  }

  @Workflow()
  @TestLifecycleAnnotation(count = 4)
  public int runWf2(int nInstances, int nWfs) {
    return 7;
  }

  @Workflow()
  public int doNotRunWF(int nInstances, int nWfs) {
    throw new IllegalStateException();
  }
}

class TestLifecycleService implements DBOSLifecycleListener {
  public int launchCount = 0;
  public int shutdownCount = 0;
  public int nInstances = 0;
  public int nWfs = 0;
  public int annotationCount = 0;

  public ArrayList<RegisteredWorkflow> wfs = new ArrayList<>();

  @Override
  public void dbosLaunched() {
    var expectedParams = new Class<?>[] {int.class, int.class};

    ++launchCount;

    nInstances = DBOS.getRegisteredWorkflowInstances().size();
    var wfs = DBOS.getRegisteredWorkflows();
    for (var wf : wfs) {
      var method = wf.workflowMethod();
      var tag = method.getAnnotation(TestLifecycleAnnotation.class);
      if (tag == null) {
        continue;
      }

      ++nWfs;
      annotationCount += tag.count();

      var paramTypes = method.getParameterTypes();
      if (!Arrays.equals(paramTypes, expectedParams)) {
        continue;
      }

      this.wfs.add(wf);
    }
  }

  @Override
  public void dbosShutDown() {
    ++shutdownCount;
  }

  public int runThemAll() throws Exception {
    int total = 0;
    for (var wf : wfs) {
      Object[] args = {nInstances, nWfs};
      var h = DBOS.startWorkflow(wf, args, new StartWorkflowOptions(UUID.randomUUID().toString()));
      total += (Integer) h.getResult();
    }
    return total;
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class LifecycleTest {
  private static DBOSConfig dbosConfig;
  private static LifecycleTestWorkflowsImpl impl;
  private static TestLifecycleService svc;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    dbosConfig =
        DBOSConfig.defaultsFromEnv("lifecycletest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys");
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOSTestAccess.reinitialize(dbosConfig);

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
  }

  @Test
  void checkThatItAllHappened() throws Exception {
    // Pretend this is an external event
    var total = svc.runThemAll();
    assertEquals(2, impl.nInstances);
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
    assertEquals(2, impl.nInstances);
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
