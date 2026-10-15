package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.exceptions.DBOSUnexpectedStepException;
import dev.dbos.transact.json.DBOSJavaSerializer;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.Workflow;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface ChildEnqueueService {
  String enqueueChildOrFallBack();

  String child(String name);
}

class ChildEnqueueServiceImpl implements ChildEnqueueService {
  private final DBOS dbos;
  ChildEnqueueService self;

  ChildEnqueueServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String enqueueChildOrFallBack() {
    try {
      dbos.startWorkflow(
          () -> self.child("from parent"),
          new StartWorkflowOptions()
              .withQueue(ChildEnqueueErrorTest.QUEUE)
              .withDeduplicationId(ChildEnqueueErrorTest.DEDUPLICATION_ID));
      return "enqueued";
    } catch (DBOSQueueDuplicatedException e) {
      return "fell back";
    }
  }

  @Override
  @Workflow
  public String child(String name) {
    return name;
  }
}

/** A child enqueue that collides on its deduplication ID is recorded as the parent's step. */
class ChildEnqueueErrorTest {
  static final String QUEUE = "child-queue";
  static final String DEDUPLICATION_ID = "held";

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  ChildEnqueueService proxy;

  @BeforeEach
  void beforeEach() {
    dataSource = pgContainer.dataSource();
  }

  private void launch() {
    launch(pgContainer.dbosConfig());
  }

  private void launch(DBOSConfig config) {
    dbos = new DBOS(config);
    var impl = new ChildEnqueueServiceImpl(dbos);
    proxy = dbos.registerProxy(ChildEnqueueService.class, impl);
    impl.self = proxy;
    dbos.launch();
    dbos.registerQueue(QUEUE, new QueueOptions());
  }

  // Runs the parent while the holder holds the deduplication ID, so its child enqueue collides
  // and the parent falls back; then lets the holder finish, freeing the ID.
  private void collideThenFreeTheId(String parentId) throws Exception {
    var queues = DBOSTestAccess.getQueueService(dbos);
    queues.setSpeedupForTest();
    // The holder waits on the paused queue, holding the deduplication ID.
    queues.pause();
    var holder =
        dbos.startWorkflow(
            () -> proxy.child("holder"),
            new StartWorkflowOptions().withQueue(QUEUE).withDeduplicationId(DEDUPLICATION_ID));

    try (var _o = new WorkflowOptions(parentId).setContext()) {
      assertEquals("fell back", proxy.enqueueChildOrFallBack());
    }

    // The holder finishes and releases the ID, so the enqueue would now succeed.
    queues.unpause();
    assertEquals("holder", holder.getResult());
  }

  // Stands in for a crash: the parent is PENDING again, and a relaunch recovers it.
  private void recover(String parentId) throws Exception {
    recover(parentId, pgContainer.dbosConfig());
  }

  private void recover(String parentId, DBOSConfig config) throws Exception {
    dbos.close();
    DBUtils.setWorkflowState(dataSource, parentId, "PENDING");
    launch(config);
  }

  /**
   * A serializer that keeps values as the native one does but loses an error's type, as a custom
   * serializer may: every error reads back as a RuntimeException.
   */
  private static final class TypeLosingSerializer implements DBOSSerializer {
    @Override
    public String name() {
      return "type-losing";
    }

    @Override
    public String serialize(Object value) {
      return DBOSJavaSerializer.INSTANCE.serialize(value);
    }

    @Override
    public Object deserialize(String text) {
      return DBOSJavaSerializer.INSTANCE.deserialize(text);
    }

    @Override
    public String serializeThrowable(Throwable throwable) {
      return throwable.getMessage();
    }

    @Override
    public Throwable deserializeThrowable(String text) {
      return new RuntimeException(text);
    }
  }

  // Records the parent's first step under another name, as if the workflow's code had changed so
  // that a different step ran there first.
  private void renameStep(String parentId) throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE dbos.operation_outputs SET function_name = 'someOtherStep'"
                    + " WHERE workflow_uuid = ? AND function_id = 0")) {
      stmt.setString(1, parentId);
      assertEquals(1, stmt.executeUpdate());
    }
  }

  @Test
  void aReplayAfterTheHolderFinishesStillSeesTheCollision() throws Exception {
    launch();
    var parentId = "parent";
    collideThenFreeTheId(parentId);

    var step = dbos.listWorkflowSteps(parentId).get(0);
    assertNotNull(step.error(), "the collision was not recorded as the parent's step");
    assertEquals(DBOSQueueDuplicatedException.class.getName(), step.error().className());
    assertNull(step.childWorkflowId());

    // The replay must see the collision it saw the first time.
    recover(parentId);
    assertEquals(
        "fell back", dbos.<String, RuntimeException>retrieveWorkflow(parentId).getResult());
    // Only the holder and the parent exist: the replay enqueued no child.
    assertEquals(2, DBUtils.getWorkflowRows(dataSource).size());
  }

  @Test
  void aCollisionReplaysAsItsOwnTypeUnderACustomSerializer() throws Exception {
    var config = pgContainer.dbosConfig().withSerializer(new TypeLosingSerializer());
    launch(config);
    var parentId = "custom-serializer";
    collideThenFreeTheId(parentId);

    // The replay must rebuild the collision the parent catches, not the serializer's stand-in.
    recover(parentId, config);
    assertEquals(
        "fell back", dbos.<String, RuntimeException>retrieveWorkflow(parentId).getResult());
    assertEquals(2, DBUtils.getWorkflowRows(dataSource).size());
  }

  @Test
  void aChildRecordedWithAnEmptyErrorIsReturned() throws Exception {
    launch();
    var parentId = "empty-error";
    try (var _o = new WorkflowOptions(parentId).setContext()) {
      assertEquals("enqueued", proxy.enqueueChildOrFallBack());
    }
    // An empty error, which some writers record for none.
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE dbos.operation_outputs SET error = ''"
                    + " WHERE workflow_uuid = ? AND function_id = 0")) {
      stmt.setString(1, parentId);
      assertEquals(1, stmt.executeUpdate());
    }

    recover(parentId);
    assertEquals("enqueued", dbos.<String, RuntimeException>retrieveWorkflow(parentId).getResult());
  }

  @Test
  void aChildRecordedByAnotherStepIsNotReturnedAsThisOnes() throws Exception {
    launch();
    var parentId = "renamed-child";
    // Nothing holds the ID, so the parent enqueues its child and records it.
    try (var _o = new WorkflowOptions(parentId).setContext()) {
      assertEquals("enqueued", proxy.enqueueChildOrFallBack());
    }
    renameStep(parentId);

    recover(parentId);
    assertThrows(
        DBOSUnexpectedStepException.class,
        () -> dbos.<String, RuntimeException>retrieveWorkflow(parentId).getResult());
  }

  @Test
  void aCollisionRecordedByAnotherStepIsNotThrownAsThisOnes() throws Exception {
    launch();
    var parentId = "renamed";
    collideThenFreeTheId(parentId);

    renameStep(parentId);

    recover(parentId);
    assertThrows(
        DBOSUnexpectedStepException.class,
        () -> dbos.<String, RuntimeException>retrieveWorkflow(parentId).getResult());
  }
}
