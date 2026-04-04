package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface TestLifecycleAnnotation {
  int count() default 4;
}

interface LifecycleTestWorkflows {
  int runWf1(int nClasses, int nWfs);

  int runWf2(int nClasses, int nWfs);

  int doNotRunWF(int nClasses, int nWfs);
}

class LifecycleTestWorkflowsImpl implements LifecycleTestWorkflows {
  int nWfs = 0, nInstances = 0;

  @Override
  @Workflow
  @TestLifecycleAnnotation(count = 3)
  public int runWf1(int nInstances, int nWfs) {
    this.nInstances = nInstances;
    this.nWfs = nWfs;
    return 8;
  }

  @Override
  @Workflow
  @TestLifecycleAnnotation(count = 4)
  public int runWf2(int nInstances, int nWfs) {
    return 7;
  }

  @Override
  @Workflow
  public int doNotRunWF(int nInstances, int nWfs) {
    throw new IllegalStateException();
  }
}

class TestLifecycleService implements DBOSLifecycleListener {
  private DBOS dbos;
  public int launchCount = 0;
  public int shutdownCount = 0;
  public int nInstances = 0;
  public int nWfs = 0;
  public int annotationCount = 0;

  public ArrayList<RegisteredWorkflow> wfs = new ArrayList<>();

  @Override
  public void dbosLaunched(DBOS dbos) {
    this.dbos = dbos;
    var expectedParams = new Class<?>[] {int.class, int.class};

    ++launchCount;

    nInstances = dbos.getRegisteredWorkflowInstances().size();
    var wfs = dbos.getRegisteredWorkflows();
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
      var h =
          dbos.startRegisteredWorkflow(
              wf, args, new StartWorkflowOptions(UUID.randomUUID().toString()));
      total += (Integer) h.getResult();
    }
    return total;
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class LifecycleTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
  }

  private void setup(DBOS dbos, LifecycleTestWorkflowsImpl impl, TestLifecycleService svc) {
    dbos.registerProxy(LifecycleTestWorkflows.class, impl, "inst1");
    dbos.registerLifecycleListener(svc);
    dbos.registerProxy(LifecycleTestWorkflows.class, new LifecycleTestWorkflowsImpl(), "instA");

    assertEquals(0, svc.launchCount);
    dbos.launch();
    assertEquals(1, svc.launchCount);
  }

  @Test
  void checkThatItAllHappened() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var impl = new LifecycleTestWorkflowsImpl();
      var svc = new TestLifecycleService();
      setup(dbos, impl, svc);

      // Pretend this is an external event
      var total = svc.runThemAll();
      assertEquals(2, impl.nInstances);
      assertEquals(4, impl.nWfs);
      assertEquals(14, svc.annotationCount);
      assertEquals(30, total);

      assertEquals(0, svc.shutdownCount);
      dbos.shutdown();
      assertEquals(1, svc.shutdownCount);
    }
  }

  @Test
  void deactivateLifecycleListeners() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var impl = new LifecycleTestWorkflowsImpl();
      var svc = new TestLifecycleService();
      setup(dbos, impl, svc);

      // Pretend this is an external event
      var total = svc.runThemAll();
      assertEquals(2, impl.nInstances);
      assertEquals(4, impl.nWfs);
      assertEquals(14, svc.annotationCount);
      assertEquals(30, total);

      assertEquals(0, svc.shutdownCount);
      DBOSTestAccess.getDbosExecutor(dbos).deactivateLifecycleListeners();
      assertEquals(1, svc.shutdownCount);
      dbos.shutdown();
      assertEquals(2, svc.shutdownCount);
    }
  }
}
