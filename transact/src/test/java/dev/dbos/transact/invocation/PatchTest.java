package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSUnexpectedStepException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface PatchService {
  int workflow();
}

@WorkflowClassName("PatchService")
class PatchServiceImplOne implements PatchService {
  @Override
  @Workflow
  public int workflow() {
    var a = DBOS.runStep(() -> 1, "stepOne");
    var b = DBOS.runStep(() -> 2, "stepTwo");
    return a + b;
  }
}

@WorkflowClassName("PatchService")
class PatchServiceImplTwo implements PatchService {
  @Override
  @Workflow
  public int workflow() {
    var a =
        DBOS.patch("v2") ? DBOS.runStep(() -> 3, "stepThree") : DBOS.runStep(() -> 1, "stepOne");
    var b = DBOS.runStep(() -> 2, "stepTwo");
    return a + b;
  }
}

@WorkflowClassName("PatchService")
class PatchServiceImplThree implements PatchService {
  @Override
  @Workflow
  public int workflow() {
    var a =
        DBOS.patch("v3")
            ? DBOS.runStep(() -> 2, "stepTwo")
            : DBOS.patch("v2")
                ? DBOS.runStep(() -> 3, "stepThree")
                : DBOS.runStep(() -> 1, "stepOne");
    var b = DBOS.runStep(() -> 2, "stepTwo");
    return a + b;
  }
}

@WorkflowClassName("PatchService")
class PatchServiceImplFour implements PatchService {
  @Override
  @Workflow
  public int workflow() {
    DBOS.deprecatePatch("v3");
    var a = DBOS.runStep(() -> 2, "stepTwo");
    var b = DBOS.runStep(() -> 2, "stepTwo");
    return a + b;
  }
}

@WorkflowClassName("PatchService")
class PatchServiceImplFive implements PatchService {
  @Override
  @Workflow
  public int workflow() {
    var a = DBOS.runStep(() -> 2, "stepTwo");
    var b = DBOS.runStep(() -> 2, "stepTwo");
    return a + b;
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class PatchTest {

  private static final Logger logger = LoggerFactory.getLogger(PatchTest.class);

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  public void testPatch() throws Exception {

    // Note, for this test we have to manually update the workflow name when forking across
    // versions. This requires pausing and unpausing the queue service to ensure the forked
    // workflow isn't executed until the workflow name is updated.

    // This hack is required because we can't have multiple service implementations with the same
    // name the way you can in a dynamic programming language like python.

    // In production, developers would be expected to be updating services in place, so they would
    // have the same workflow name across deployed versions.

    var dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withEnablePatching()
            .withAppVersion("test-version");

    try (var dataSource = SystemDatabase.createDataSource(dbosConfig)) {

      DBUtils.recreateDB(dbosConfig);
      DBOS.reinitialize(dbosConfig);

      var proxy1 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplOne());
      DBOS.launch();

      assertEquals("test-version", DBOSTestAccess.getDbosExecutor().appVersion());
      var queueService = DBOSTestAccess.getQueueService();

      // Register and run the first version of a workflow
      var h1 = DBOS.startWorkflow(() -> proxy1.workflow(), new StartWorkflowOptions("impl1"));
      assertEquals(3, h1.getResult());
      var steps = DBOS.listWorkflowSteps(h1.workflowId());
      assertEquals(2, steps.size());

      // Recreate DBOS with a new (patched) version of a workflow
      DBOS.shutdown();
      DBOS.reinitialize(dbosConfig);
      var proxy2 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplTwo());
      DBOS.launch();

      // Verify a new execution runs the post-patch workflow and stores a patch marker
      var h2 = DBOS.startWorkflow(() -> proxy2.workflow(), new StartWorkflowOptions("impl2"));
      assertEquals(5, h2.getResult());
      steps = DBOS.listWorkflowSteps(h2.workflowId());
      assertEquals(3, steps.size());
      assertEquals("DBOS.patch-v2", steps.get(0).functionName());

      // Verify an execution containing the patch marker can recover past the patch marker
      var h2Fork2 = DBOS.forkWorkflow(h2.workflowId(), 3, new ForkOptions("impl2_fork2"));
      assertEquals(5, h2Fork2.getResult());
      steps = DBOS.listWorkflowSteps(h2Fork2.workflowId());
      assertEquals(3, steps.size());
      assertEquals("DBOS.patch-v2", steps.get(0).functionName());

      // Verify an old execution runs the pre-patch workflow and does not store a patch marker
      var h2Fork1 = DBOS.forkWorkflow(h1.workflowId(), 2, new ForkOptions("impl2_fork1"));
      assertEquals(3, h2Fork1.getResult());
      assertEquals(2, DBOS.listWorkflowSteps(h2Fork1.workflowId()).size());

      // Recreate DBOS with another new (patched) version of a workflow
      DBOS.shutdown();
      DBOS.reinitialize(dbosConfig);
      var proxy3 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplThree());
      DBOS.launch();

      // Verify a new execution runs the post-patch workflow and stores a patch marker
      var h3 = DBOS.startWorkflow(() -> proxy3.workflow(), new StartWorkflowOptions("impl3"));
      assertEquals(4, h3.getResult());
      steps = DBOS.listWorkflowSteps(h3.workflowId());
      assertEquals(3, steps.size());
      assertEquals("DBOS.patch-v3", steps.get(0).functionName());

      // Verify an execution containing the v3 patch marker recovers to v3
      var h3Fork3 = DBOS.forkWorkflow(h3.workflowId(), 3, new ForkOptions("impl3_fork3"));
      assertEquals(4, h3Fork3.getResult());
      steps = DBOS.listWorkflowSteps(h3Fork3.workflowId());
      assertEquals(3, steps.size());
      assertEquals("DBOS.patch-v3", steps.get(0).functionName());

      // Verify an execution containing the v2 patch marker recovers to v2
      var h3Fork2 = DBOS.forkWorkflow(h2.workflowId(), 3, new ForkOptions("impl3_fork2"));
      assertEquals(5, h3Fork2.getResult());
      steps = DBOS.listWorkflowSteps(h3Fork2.workflowId());
      assertEquals(3, steps.size());
      assertEquals("DBOS.patch-v2", steps.get(0).functionName());

      // Verify a v1 execution recovers the pre-patch workflow and does not store a patch marker
      var h3Fork1 = DBOS.forkWorkflow(h1.workflowId(), 2, new ForkOptions("impl3_fork1"));
      assertEquals(3, h3Fork1.getResult());
      assertEquals(2, DBOS.listWorkflowSteps(h3Fork1.workflowId()).size());

      // Now, let's deprecate the patch
      DBOS.shutdown();
      DBOS.reinitialize(dbosConfig);
      var proxy4 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplFour());
      DBOS.launch();

      // Verify a new execution runs the final workflow but does not store a patch marker
      var h4 = DBOS.startWorkflow(() -> proxy4.workflow(), new StartWorkflowOptions("impl4"));
      assertEquals(4, h4.getResult());
      assertEquals(2, DBOS.listWorkflowSteps(h4.workflowId()).size());

      // Verify an execution sans patch marker recovers correctly
      var h4Fork4 = DBOS.forkWorkflow(h4.workflowId(), 3, new ForkOptions("impl4_fork4"));
      assertEquals(4, h4Fork4.getResult());
      assertEquals(2, DBOS.listWorkflowSteps(h4Fork4.workflowId()).size());

      // Verify an execution containing the v3 patch marker recovers to v3
      var h4Fork3 = DBOS.forkWorkflow(h3.workflowId(), 3, new ForkOptions("impl4_fork3"));
      assertEquals(4, h4Fork3.getResult());
      steps = DBOS.listWorkflowSteps(h4Fork3.workflowId());
      assertEquals(3, steps.size());
      assertEquals("DBOS.patch-v3", steps.get(0).functionName());

      // Verify an execution containing the v2 patch marker cleanly fails
      var h4Fork2 = DBOS.forkWorkflow(h2.workflowId(), 3, new ForkOptions("impl4_fork2"));
      assertThrows(DBOSUnexpectedStepException.class, () -> h4Fork2.getResult());

      // Verify a v1 execution cleanly fails
      var h4Fork1 = DBOS.forkWorkflow(h1.workflowId(), 2, new ForkOptions("impl4_fork1"));
      assertThrows(DBOSUnexpectedStepException.class, () -> h4Fork1.getResult());

      // Now, let's deprecate the patch
      DBOS.shutdown();
      DBOS.reinitialize(dbosConfig);
      var proxy5 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplFive());
      DBOS.launch();

      // Verify a new execution runs the final workflow but does not store a patch marker
      var h5 = DBOS.startWorkflow(() -> proxy5.workflow(), new StartWorkflowOptions("impl5"));
      assertEquals(4, h5.getResult());
      assertEquals(2, DBOS.listWorkflowSteps(h5.workflowId()).size());

      // Verify an execution from the deprecated patch works sans patch marker
      var h5Fork4 = DBOS.forkWorkflow(h4.workflowId(), 3, new ForkOptions("impl5_fork4"));
      assertEquals(4, h5Fork4.getResult());
      assertEquals(2, DBOS.listWorkflowSteps(h5Fork4.workflowId()).size());

      // Verify an execution containing the v3 patch marker cleanly fails
      var h5Fork3 = DBOS.forkWorkflow(h3.workflowId(), 3, new ForkOptions("impl5_fork3"));
      assertThrows(DBOSUnexpectedStepException.class, () -> h5Fork3.getResult());

      // Verify an execution containing the v2 patch marker cleanly fails
      var h5Fork2 = DBOS.forkWorkflow(h2.workflowId(), 3, new ForkOptions("impl5_fork2"));
      assertThrows(DBOSUnexpectedStepException.class, () -> h5Fork2.getResult());

      // Verify a v1 execution cleanly fails
      var h5Fork1 = DBOS.forkWorkflow(h1.workflowId(), 2, new ForkOptions("impl5_fork1"));
      assertThrows(DBOSUnexpectedStepException.class, () -> h5Fork1.getResult());
    }
  }

  @Test
  public void patchThrowsNotConfigured() throws Exception {
    var dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withAppVersion("test-version");

    DBUtils.recreateDB(dbosConfig);
    DBOS.reinitialize(dbosConfig);

    var proxy2 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplTwo());
    DBOS.launch();

    assertThrows(IllegalStateException.class, () -> proxy2.workflow());
  }

  @Test
  public void deprecatePatchThrowsNotConfigured() throws Exception {
    var dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withAppVersion("test-version");

    DBUtils.recreateDB(dbosConfig);
    DBOS.reinitialize(dbosConfig);

    var proxy4 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplFour());
    DBOS.launch();

    assertThrows(IllegalStateException.class, () -> proxy4.workflow());
  }
}
