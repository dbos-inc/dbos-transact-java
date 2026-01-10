package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSUnexpectedStepException;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.DBUtils.DBSettings;
import dev.dbos.transact.workflow.Workflow;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface PatchService {
  int workflow();
}

class PatchServiceImplOne implements PatchService {
  @Override
  @Workflow
  public int workflow() {
    var a = DBOS.runStep(() -> 1, "stepOne");
    var b = DBOS.runStep(() -> 2, "stepTwo");
    return a + b;
  }
}

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

    final DBSettings db = DBSettings.get();
    db.recreate();

    var dataSource = db.dataSource();
    var dbosConfig =
        DBOSConfig.defaults("systemdbtest")
            .withDataSource(dataSource)
            .withEnablePatching()
            .withAppVersion("test-version");

    DBOS.reinitialize(dbosConfig);

    var proxy1 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplOne());
    DBOS.launch();

    assertEquals("test-version", DBOSTestAccess.getDbosExecutor().appVersion());
    var queueService = DBOSTestAccess.getQueueService();

    // Register and run the first version of a workflow
    var h1 = DBOS.startWorkflow(() -> proxy1.workflow());
    assertEquals(3, h1.getResult());
    var steps = DBOS.listWorkflowSteps(h1.workflowId());
    assertEquals(2, steps.size());

    // Recreate DBOS with a new (patched) version of a workflow
    DBOS.shutdown();
    DBOS.reinitialize(dbosConfig);
    var proxy2 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplTwo());
    DBOS.launch();

    // Verify a new execution runs the post-patch workflow and stores a patch marker
    var h2 = DBOS.startWorkflow(() -> proxy2.workflow());
    assertEquals(5, h2.getResult());
    steps = DBOS.listWorkflowSteps(h2.workflowId());
    assertEquals(3, steps.size());
    assertEquals("DBOS.patch-v2", steps.get(0).functionName());

    // Verify an execution containing the patch marker can recover past the patch marker
    var h2Fork2 = DBOS.forkWorkflow(h2.workflowId(), 3);
    assertEquals(5, h2Fork2.getResult());
    steps = DBOS.listWorkflowSteps(h2Fork2.workflowId());
    assertEquals(3, steps.size());
    assertEquals("DBOS.patch-v2", steps.get(0).functionName());

    // Verify an old execution runs the pre-patch workflow and does not store a patch marker
    queueService.pause();
    var h2Fork1 = DBOS.forkWorkflow(h1.workflowId(), 2);
    updateWorkflowName(dataSource, h2.workflowId(), h2Fork1.workflowId());
    queueService.unpause();
    assertEquals(3, h2Fork1.getResult());
    assertEquals(2, DBOS.listWorkflowSteps(h2Fork1.workflowId()).size());

    // Recreate DBOS with another new (patched) version of a workflow
    DBOS.shutdown();
    DBOS.reinitialize(dbosConfig);
    var proxy3 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplThree());
    DBOS.launch();

    // Verify a new execution runs the post-patch workflow and stores a patch marker
    var h3 = DBOS.startWorkflow(() -> proxy3.workflow());
    assertEquals(4, h3.getResult());
    steps = DBOS.listWorkflowSteps(h3.workflowId());
    assertEquals(3, steps.size());
    assertEquals("DBOS.patch-v3", steps.get(0).functionName());

    // Verify an execution containing the v3 patch marker recovers to v3
    var h3Fork3 = DBOS.forkWorkflow(h3.workflowId(), 3);
    assertEquals(4, h3Fork3.getResult());
    steps = DBOS.listWorkflowSteps(h3Fork3.workflowId());
    assertEquals(3, steps.size());
    assertEquals("DBOS.patch-v3", steps.get(0).functionName());

    // Verify an execution containing the v2 patch marker recovers to v2
    queueService.pause();
    var h3Fork2 = DBOS.forkWorkflow(h2.workflowId(), 3);
    updateWorkflowName(dataSource, h3.workflowId(), h3Fork2.workflowId());
    queueService.unpause();
    assertEquals(5, h3Fork2.getResult());
    steps = DBOS.listWorkflowSteps(h3Fork2.workflowId());
    assertEquals(3, steps.size());
    assertEquals("DBOS.patch-v2", steps.get(0).functionName());

    // Verify a v1 execution recovers the pre-patch workflow and does not store a patch marker
    queueService.pause();
    var h3Fork1 = DBOS.forkWorkflow(h1.workflowId(), 2);
    updateWorkflowName(dataSource, h3.workflowId(), h3Fork1.workflowId());
    queueService.unpause();
    assertEquals(3, h3Fork1.getResult());
    assertEquals(2, DBOS.listWorkflowSteps(h3Fork1.workflowId()).size());

    // Now, let's deprecate the patch
    DBOS.shutdown();
    DBOS.reinitialize(dbosConfig);
    var proxy4 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplFour());
    DBOS.launch();

    // Verify a new execution runs the final workflow but does not store a patch marker
    var h4 = DBOS.startWorkflow(() -> proxy4.workflow());
    assertEquals(4, h4.getResult());
    assertEquals(2, DBOS.listWorkflowSteps(h4.workflowId()).size());

    // Verify an execution sans patch marker recovers correctly
    var h4Fork4 = DBOS.forkWorkflow(h4.workflowId(), 3);
    assertEquals(4, h4Fork4.getResult());
    assertEquals(2, DBOS.listWorkflowSteps(h4Fork4.workflowId()).size());

    // Verify an execution containing the v3 patch marker recovers to v3
    queueService.pause();
    var h4Fork3 = DBOS.forkWorkflow(h3.workflowId(), 3);
    updateWorkflowName(dataSource, h4.workflowId(), h4Fork3.workflowId());
    queueService.unpause();
    assertEquals(4, h4Fork3.getResult());
    steps = DBOS.listWorkflowSteps(h4Fork3.workflowId());
    assertEquals(3, steps.size());
    assertEquals("DBOS.patch-v3", steps.get(0).functionName());

    // Verify an execution containing the v2 patch marker cleanly fails
    queueService.pause();
    var h4Fork2 = DBOS.forkWorkflow(h2.workflowId(), 3);
    updateWorkflowName(dataSource, h4.workflowId(), h4Fork2.workflowId());
    queueService.unpause();
    assertThrows(DBOSUnexpectedStepException.class, () -> h4Fork2.getResult());

    // Verify a v1 execution cleanly fails
    queueService.pause();
    var h4Fork1 = DBOS.forkWorkflow(h1.workflowId(), 2);
    updateWorkflowName(dataSource, h4.workflowId(), h4Fork1.workflowId());
    queueService.unpause();
    assertThrows(DBOSUnexpectedStepException.class, () -> h4Fork1.getResult());

    // Now, let's deprecate the patch
    DBOS.shutdown();
    DBOS.reinitialize(dbosConfig);
    var proxy5 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplFive());
    DBOS.launch();

    // Verify a new execution runs the final workflow but does not store a patch marker
    var h5 = DBOS.startWorkflow(() -> proxy5.workflow());
    assertEquals(4, h5.getResult());
    assertEquals(2, DBOS.listWorkflowSteps(h5.workflowId()).size());

    // Verify an execution from the deprecated patch works sans patch marker
    queueService.pause();
    var h5Fork4 = DBOS.forkWorkflow(h4.workflowId(), 3);
    updateWorkflowName(dataSource, h5.workflowId(), h5Fork4.workflowId());
    queueService.unpause();
    assertEquals(4, h5Fork4.getResult());
    assertEquals(2, DBOS.listWorkflowSteps(h5Fork4.workflowId()).size());

    // Verify an execution containing the v3 patch marker cleanly fails
    queueService.pause();
    var h5Fork3 = DBOS.forkWorkflow(h3.workflowId(), 3);
    updateWorkflowName(dataSource, h5.workflowId(), h5Fork3.workflowId());
    queueService.unpause();
    assertThrows(DBOSUnexpectedStepException.class, () -> h5Fork3.getResult());

    // Verify an execution containing the v2 patch marker cleanly fails
    queueService.pause();
    var h5Fork2 = DBOS.forkWorkflow(h2.workflowId(), 3);
    updateWorkflowName(dataSource, h5.workflowId(), h5Fork2.workflowId());
    queueService.unpause();
    assertThrows(DBOSUnexpectedStepException.class, () -> h5Fork2.getResult());

    // Verify a v1 execution cleanly fails
    queueService.pause();
    var h5Fork1 = DBOS.forkWorkflow(h1.workflowId(), 2);
    updateWorkflowName(dataSource, h5.workflowId(), h5Fork1.workflowId());
    queueService.unpause();
    assertThrows(DBOSUnexpectedStepException.class, () -> h5Fork1.getResult());
  }

  void updateWorkflowName(DataSource dataSource, String sourceId, String destinationId)
      throws SQLException {

    var row = DBUtils.getWorkflowRow(dataSource, sourceId);
    if (row == null) {
      throw new RuntimeException("Source workflow %s not found".formatted(sourceId));
    }

    logger.info(
        "updateWorkflowName {} workflow to {}",
        destinationId,
        RegisteredWorkflow.fullyQualifiedName(row.className(), row.instanceName(), row.name()));

    var sql =
        """
          UPDATE %s.workflow_status
          SET name = ?, class_name = ?, config_name = ?
          WHERE workflow_uuid = ?
        """
            .formatted(SystemDatabase.sanitizeSchema(null));

    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(sql)) {
      ps.setString(1, row.name());
      ps.setString(2, row.className());
      ps.setString(3, row.instanceName());
      ps.setString(4, destinationId);

      if (ps.executeUpdate() == 0) {
        logger.warn("updateWorkflowName {} workflow updated 0 rows", destinationId);
      }
    }
  }

  @Test
  public void patchThrowsNotConfigured() throws Exception {
    final DBSettings db = DBSettings.get();
    db.recreate();

    var dataSource = db.dataSource();
    var dbosConfig = DBOSConfig.defaults("systemdbtest").withDataSource(dataSource);

    DBOS.reinitialize(dbosConfig);

    var proxy2 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplTwo());
    DBOS.launch();

    assertThrows(IllegalStateException.class, () -> proxy2.workflow());
  }

  @Test
  public void deprecatePatchThrowsNotConfigured() throws Exception {
    final DBSettings db = DBSettings.get();
    db.recreate();

    var dataSource = db.dataSource();
    var dbosConfig = DBOSConfig.defaults("systemdbtest").withDataSource(dataSource);

    DBOS.reinitialize(dbosConfig);

    var proxy4 = DBOS.registerWorkflows(PatchService.class, new PatchServiceImplFour());
    DBOS.launch();

    assertThrows(IllegalStateException.class, () -> proxy4.workflow());
  }
}
