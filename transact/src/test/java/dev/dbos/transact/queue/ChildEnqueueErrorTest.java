package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
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
    dbos = new DBOS(pgContainer.dbosConfig());
    var impl = new ChildEnqueueServiceImpl(dbos);
    proxy = dbos.registerProxy(ChildEnqueueService.class, impl);
    impl.self = proxy;
    dbos.launch();
    dbos.registerQueue(QUEUE, new QueueOptions());
  }

  @Test
  void aReplayAfterTheHolderFinishesStillSeesTheCollision() throws Exception {
    launch();
    var queues = DBOSTestAccess.getQueueService(dbos);
    queues.setSpeedupForTest();
    // The holder waits on the paused queue, holding the deduplication ID.
    queues.pause();
    var holder =
        dbos.startWorkflow(
            () -> proxy.child("holder"),
            new StartWorkflowOptions().withQueue(QUEUE).withDeduplicationId(DEDUPLICATION_ID));

    var parentId = "parent";
    try (var _o = new WorkflowOptions(parentId).setContext()) {
      assertEquals("fell back", proxy.enqueueChildOrFallBack());
    }
    var step = dbos.listWorkflowSteps(parentId).get(0);
    assertNotNull(step.error(), "the collision was not recorded as the parent's step");
    assertEquals(DBOSQueueDuplicatedException.class.getName(), step.error().className());
    assertNull(step.childWorkflowId());

    // The holder finishes and releases the ID, so the enqueue would now succeed.
    queues.unpause();
    assertEquals("holder", holder.getResult());

    // A recovery replays the parent, which must see the collision it saw the first time.
    dbos.close();
    DBUtils.setWorkflowState(dataSource, parentId, "PENDING");
    launch();
    assertEquals(
        "fell back", dbos.<String, RuntimeException>retrieveWorkflow(parentId).getResult());
    // Only the holder and the parent exist: the replay enqueued no child.
    assertEquals(2, DBUtils.getWorkflowRows(dataSource).size());
  }
}
