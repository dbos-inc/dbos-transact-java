package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueConflictResolution;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DynamicQueuesTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();
  }

  @Test
  public void testDynamicQueueWorkflowExecution() throws Exception {
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();

    // Register a dynamic queue after launch — this writes to DB.
    dbos.registerQueue("dynQueue", QueueOptions.empty());

    // The supervisor polls every 1s; wait for it to discover and start a listener.
    var handle =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("hello"),
            new StartWorkflowOptions().withQueue("dynQueue"));

    assertEquals("hellohello", handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void testDynamicQueueConcurrency() throws Exception {
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();

    dbos.registerQueue("concQ", QueueOptions.setConcurrency(1).andWorkerConcurrency(1));

    for (int i = 0; i < 3; i++) {
      String id = "dynwf" + i;
      String input = "v" + i;
      dbos.startWorkflow(
          () -> serviceQ.simpleQWorkflow(input), new StartWorkflowOptions(id).withQueue("concQ"));
    }

    for (int i = 0; i < 3; i++) {
      var handle = dbos.retrieveWorkflow("dynwf" + i);
      assertEquals("v" + i + "v" + i, handle.getResult());
      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
    }
  }

  @Test
  public void testListQueues() throws Exception {
    dbos.launch();

    dbos.registerQueue("q-list-1", QueueOptions.setConcurrency(1));
    dbos.registerQueue("q-list-2", QueueOptions.setConcurrency(2));
    dbos.registerQueue("q-list-3", QueueOptions.empty());

    var queues = dbos.listQueues();
    var names = queues.stream().map(Queue::name).toList();
    assertTrue(names.contains("q-list-1"));
    assertTrue(names.contains("q-list-2"));
    assertTrue(names.contains("q-list-3"));
    assertEquals(3, names.size());
  }

  @Test
  public void testDeleteQueue() throws Exception {
    dbos.launch();

    dbos.registerQueue("q-del", QueueOptions.setConcurrency(1));
    assertTrue(dbos.listQueues().stream().anyMatch(q -> q.name().equals("q-del")));

    boolean deleted = dbos.deleteQueue("q-del");
    assertTrue(deleted);
    assertFalse(dbos.listQueues().stream().anyMatch(q -> q.name().equals("q-del")));

    // deleting a non-existent queue returns false
    assertFalse(dbos.deleteQueue("q-never-existed"));
  }

  @Test
  public void testUpdateQueue() throws Exception {
    dbos.launch();

    dbos.registerQueue("q-update", QueueOptions.setConcurrency(5));

    var before =
        dbos.listQueues().stream()
            .filter(x -> x.name().equals("q-update"))
            .findFirst()
            .orElseThrow();
    assertEquals(5, before.concurrency());

    dbos.updateQueue("q-update", QueueOptions.setConcurrency(10));

    var after =
        dbos.listQueues().stream()
            .filter(x -> x.name().equals("q-update"))
            .findFirst()
            .orElseThrow();
    assertEquals(10, after.concurrency());
  }

  @Test
  public void testRegisterQueueNeverUpdate() throws Exception {
    dbos.launch();

    dbos.registerQueue("q-conflict", QueueOptions.setConcurrency(5));

    // NEVER_UPDATE: second call should not overwrite
    dbos.registerQueue(
        "q-conflict", QueueOptions.setConcurrency(99), QueueConflictResolution.NEVER_UPDATE);

    var q =
        dbos.listQueues().stream()
            .filter(x -> x.name().equals("q-conflict"))
            .findFirst()
            .orElseThrow();
    assertEquals(5, q.concurrency());
  }

  @Test
  public void testRegisterQueueAlwaysUpdate() throws Exception {
    dbos.launch();

    dbos.registerQueue("q-always", QueueOptions.setConcurrency(5));

    // ALWAYS_UPDATE: second call should overwrite
    dbos.registerQueue(
        "q-always", QueueOptions.setConcurrency(99), QueueConflictResolution.ALWAYS_UPDATE);

    var q =
        dbos.listQueues().stream()
            .filter(x -> x.name().equals("q-always"))
            .findFirst()
            .orElseThrow();
    assertEquals(99, q.concurrency());
  }

  @Test
  public void testDynamicQueuePollingInterval() throws Exception {
    dbos.launch();

    var interval = Duration.ofSeconds(3);
    dbos.registerQueue("q-poll", QueueOptions.setPollingInterval(interval));

    var q =
        dbos.listQueues().stream().filter(x -> x.name().equals("q-poll")).findFirst().orElseThrow();
    assertEquals(interval, q.pollingInterval());
  }

  @Test
  public void testRegisterInternalQueueThrows() throws Exception {
    dbos.launch();

    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.registerQueue("_dbos_internal_queue", QueueOptions.empty()));
  }

  @Test
  public void testDeleteAndRecreateQueue() throws Exception {
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();

    dbos.registerQueue("q-lifecycle", QueueOptions.setConcurrency(5));

    var h1 =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("first"),
            new StartWorkflowOptions("lc-wf1").withQueue("q-lifecycle"));
    assertEquals("firstfirst", h1.getResult());

    dbos.deleteQueue("q-lifecycle");
    assertFalse(dbos.listQueues().stream().anyMatch(x -> x.name().equals("q-lifecycle")));

    // Wait for the old listener to detect the deletion and remove itself from the
    // active-listener set. Without this wait the supervisor may not start a fresh
    // listener for the recreated queue (dbListeningQueues still contains the name).
    Thread.sleep(500);

    // Recreate with different config.
    dbos.registerQueue("q-lifecycle", QueueOptions.setConcurrency(2));
    var recreated =
        dbos.listQueues().stream()
            .filter(x -> x.name().equals("q-lifecycle"))
            .findFirst()
            .orElseThrow();
    assertEquals(2, recreated.concurrency());

    var h2 =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("second"),
            new StartWorkflowOptions("lc-wf2").withQueue("q-lifecycle"));
    assertEquals("secondsecond", h2.getResult());
  }

  @Test
  public void testStaticAndDynamicQueueSameName() throws Exception {
    // Static queue registered pre-launch.
    var staticQ = new Queue("q-shared").withConcurrency(3);
    dbos.registerQueue(staticQ);
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();

    // Register same name as a dynamic queue — supervisor should ignore the DB entry.
    dbos.registerQueue("q-shared", QueueOptions.setConcurrency(99));

    // Workflow still executes — static listener handles it.
    var handle =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("shared"),
            new StartWorkflowOptions().withQueue("q-shared"));
    assertEquals("sharedshared", handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void testDynamicConcurrencyTakesEffect() throws Exception {
    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();

    // Start with concurrency=1 so only one workflow dequeues at a time.
    dbos.registerQueue("dyn-update-q", QueueOptions.setConcurrency(1));

    var h1 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0),
            new StartWorkflowOptions("dyn-wf1").withQueue("dyn-update-q"));
    var h2 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(1),
            new StartWorkflowOptions("dyn-wf2").withQueue("dyn-update-q"));
    var h3 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(2),
            new StartWorkflowOptions("dyn-wf3").withQueue("dyn-update-q"));

    // Wait for exactly one workflow to be dequeued and start running.
    impl.wfSemaphore.acquire(1);

    // With concurrency=1 the other two should still be waiting.
    Thread.sleep(200);
    assertEquals(WorkflowState.ENQUEUED, h2.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, h3.getStatus().status());

    // Bump concurrency. The runner reloads queue settings on its next poll and
    // should immediately dequeue the remaining two workflows.
    dbos.updateQueue("dyn-update-q", QueueOptions.setConcurrency(3));
    impl.wfSemaphore.acquire(2);

    // Release all blocked workflows and verify they complete successfully.
    impl.latch.countDown();
    assertEquals(0, h1.getResult());
    assertEquals(1, h2.getResult());
    assertEquals(2, h3.getResult());
  }

  @Test
  public void testDynamicQueueMapUpdatedOnRegister() throws Exception {
    dbos.launch();
    var qs = DBOSTestAccess.getQueueService(dbos);

    assertFalse(qs.findDynamicQueue("q-map-reg").isPresent());

    dbos.registerQueue("q-map-reg", QueueOptions.setConcurrency(5));
    awaitCondition(() -> qs.findDynamicQueue("q-map-reg").isPresent());

    assertEquals(5, qs.findDynamicQueue("q-map-reg").get().concurrency());
  }

  @Test
  public void testDynamicQueueMapUpdatedOnUpdate() throws Exception {
    dbos.launch();
    var qs = DBOSTestAccess.getQueueService(dbos);

    dbos.registerQueue("q-map-upd", QueueOptions.setConcurrency(5));
    awaitCondition(() -> qs.findDynamicQueue("q-map-upd").isPresent());

    dbos.updateQueue("q-map-upd", QueueOptions.setConcurrency(10));
    awaitCondition(
        () ->
            qs.findDynamicQueue("q-map-upd")
                .filter(q -> Integer.valueOf(10).equals(q.concurrency()))
                .isPresent());

    assertEquals(10, qs.findDynamicQueue("q-map-upd").get().concurrency());
  }

  @Test
  public void testDynamicQueueMapUpdatedOnDelete() throws Exception {
    dbos.launch();
    var qs = DBOSTestAccess.getQueueService(dbos);

    dbos.registerQueue("q-map-del", QueueOptions.empty());
    awaitCondition(() -> qs.findDynamicQueue("q-map-del").isPresent());

    dbos.deleteQueue("q-map-del");
    awaitCondition(() -> qs.findDynamicQueue("q-map-del").isEmpty());
  }

  private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 2000;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError("Condition not met within 2s");
      Thread.sleep(50);
    }
  }

  @Test
  public void testRegisterQueueValidation() throws Exception {
    dbos.launch();

    // Zero or negative polling interval should fail.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.registerQueue(
                "q-bad-poll", QueueOptions.setPollingInterval(Duration.ofSeconds(-1))));
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.registerQueue("q-bad-poll", QueueOptions.setPollingInterval(Duration.ZERO)));

    // Zero or negative concurrency should fail.
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.registerQueue("q-bad-conc", QueueOptions.setConcurrency(0)));
  }
}
