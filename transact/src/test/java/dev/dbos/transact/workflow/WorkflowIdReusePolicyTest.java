package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.EnqueueOptions;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.exceptions.DBOSWorkflowIdInUseException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.time.Duration;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

interface ReuseService {
  String quick(String value);

  String fail();

  String waitForMessage();

  String startChild(String childId, boolean queued);
}

class ReuseServiceImpl implements ReuseService {
  private final DBOS dbos;
  ReuseService proxy;

  ReuseServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String quick(String value) {
    return value;
  }

  @Override
  @Workflow
  public String fail() {
    throw new IllegalStateException("failed");
  }

  @Override
  @Workflow
  public String waitForMessage() {
    return dbos.<String>recv("go", Duration.ofMinutes(1)).orElse("timed out");
  }

  @Override
  @Workflow
  public String startChild(String childId, boolean queued) {
    var options =
        new StartWorkflowOptions()
            .withWorkflowId(childId)
            .withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT);
    if (queued) {
      options = options.withQueue(WorkflowIdReusePolicyTest.QUEUE);
    }
    try {
      return dbos.startWorkflow(() -> proxy.quick("child"), options).getResult();
    } catch (DBOSWorkflowIdInUseException e) {
      return "in use: " + e.workflowStatus();
    }
  }
}

/**
 * {@link WorkflowIdReusePolicy#REJECT} refuses a start or enqueue whose workflow ID is already
 * taken, whatever the holder's status, and writes nothing. {@link
 * WorkflowIdReusePolicy#RETURN_EXISTING}, the default, attaches to the holder as before.
 */
public class WorkflowIdReusePolicyTest {

  static final String QUEUE = "reuse-policy-queue";

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOS dbos;
  ReuseService proxy;

  @BeforeEach
  void beforeEach() {
    dataSource = pgContainer.dataSource();
    dbos = new DBOS(pgContainer.dbosConfig());
    var impl = new ReuseServiceImpl(dbos);
    proxy = dbos.registerProxy(ReuseService.class, impl);
    impl.proxy = proxy;
    dbos.launch();
    dbos.registerQueue(QUEUE, new QueueOptions());
  }

  private static StartWorkflowOptions withId(String workflowId) {
    return new StartWorkflowOptions().withWorkflowId(workflowId);
  }

  private static StartWorkflowOptions rejecting(String workflowId) {
    return withId(workflowId).withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT);
  }

  /** Starts a workflow holding {@code workflowId} that is left in {@code state}. */
  private void holdId(String workflowId, WorkflowState state) throws Exception {
    switch (state) {
      case SUCCESS ->
          dbos.startWorkflow(() -> proxy.quick("first"), withId(workflowId)).getResult();
      case ERROR -> {
        var handle = dbos.startWorkflow(() -> proxy.fail(), withId(workflowId));
        assertThrows(IllegalStateException.class, handle::getResult);
      }
      case PENDING -> dbos.startWorkflow(() -> proxy.waitForMessage(), withId(workflowId));
      default -> throw new IllegalArgumentException(state.name());
    }
  }

  @Test
  void aRejectingStartOnAFreshIdRuns() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    assertEquals(
        "fresh", dbos.startWorkflow(() -> proxy.quick("fresh"), rejecting(workflowId)).getResult());
  }

  @ParameterizedTest
  @EnumSource(
      value = WorkflowState.class,
      names = {"PENDING", "SUCCESS", "ERROR"})
  void aRejectingStartOnATakenIdIsRefusedAndWritesNothing(WorkflowState holderState)
      throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, holderState);
    var before = DBUtils.getWorkflowRow(dataSource, workflowId);

    var refused =
        assertThrows(
            DBOSWorkflowIdInUseException.class,
            () -> dbos.startWorkflow(() -> proxy.quick("second"), rejecting(workflowId)));

    assertEquals(workflowId, refused.workflowId());
    assertEquals(holderState, refused.workflowStatus());
    assertEquals(before.workflowName(), refused.workflowName());
    var after = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertEquals(before.inputs(), after.inputs(), "the holder's input is untouched");
    assertEquals(before.updatedAt(), after.updatedAt(), "the holder's row is untouched");

    if (holderState == WorkflowState.PENDING) {
      dbos.send(workflowId, "done", "go");
    }
  }

  @Test
  void aRejectingEnqueueOnATakenIdIsRefused() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    assertThrows(
        DBOSWorkflowIdInUseException.class,
        () ->
            dbos.startWorkflow(
                () -> proxy.quick("second"), rejecting(workflowId).withQueue(QUEUE)));
  }

  @Test
  void aRejectingClientEnqueueOnATakenIdIsRefused() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    try (var client = pgContainer.dbosClient()) {
      var options =
          new EnqueueOptions("quick", QueueName.of(QUEUE))
              .withWorkflowId(workflowId)
              .withWorkflowIdReusePolicy(WorkflowIdReusePolicy.REJECT);
      var refused =
          assertThrows(
              DBOSWorkflowIdInUseException.class,
              () -> client.enqueueWorkflow(options, new Object[] {"second"}));
      assertEquals(WorkflowState.SUCCESS, refused.workflowStatus());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void returnExistingAttachesToTheHolder(boolean explicit) throws Exception {
    var workflowId = UUID.randomUUID().toString();
    holdId(workflowId, WorkflowState.SUCCESS);
    var options =
        explicit
            ? withId(workflowId).withWorkflowIdReusePolicy(WorkflowIdReusePolicy.RETURN_EXISTING)
            : withId(workflowId);
    assertEquals("first", dbos.startWorkflow(() -> proxy.quick("second"), options).getResult());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void aRefusedChildIsRecordedAtItsParentsStepAndReplayed(boolean queued) throws Exception {
    var childId = UUID.randomUUID().toString();
    holdId(childId, WorkflowState.SUCCESS);
    var parentId = UUID.randomUUID().toString();

    assertEquals(
        "in use: SUCCESS",
        dbos.startWorkflow(() -> proxy.startChild(childId, queued), withId(parentId)).getResult());

    var steps = dbos.listWorkflowSteps(parentId);
    assertEquals(1, steps.size());
    assertNotNull(steps.get(0).error(), "the refusal is recorded at the parent's step");
    assertTrue(
        steps.get(0).error().className().endsWith("DBOSWorkflowIdInUseException"),
        steps.get(0).error().className());

    // A replay throws the recorded refusal, even once the ID is free again.
    dbos.deleteWorkflow(childId);
    assertEquals(
        "in use: SUCCESS", dbos.<String, RuntimeException>forkWorkflow(parentId, 1).getResult());
  }
}
