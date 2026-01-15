package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface ResumingTestService {
  int stuckWorkflow() throws InterruptedException;

  int regularWorkflow();
}

class ResumingTestServiceImpl implements ResumingTestService {

  public CountDownLatch startLatch = new CountDownLatch(1);
  public CountDownLatch blockingLatch = new CountDownLatch(1);

  @Override
  @Workflow
  public int stuckWorkflow() throws InterruptedException {
    startLatch.countDown();
    blockingLatch.await();
    return 13;
  }

  @Override
  @Workflow
  public int regularWorkflow() {
    return 42;
  }
}

interface PartitionsTestService {
  String blockedWorkflow() throws InterruptedException;

  String normalWorkflow();
}

class PartitionsTestServiceImpl implements PartitionsTestService {
  public CountDownLatch waitingLatch = new CountDownLatch(1);
  public CountDownLatch blockingLatch = new CountDownLatch(1);

  @Override
  @Workflow
  public String blockedWorkflow() throws InterruptedException {
    waitingLatch.countDown();
    blockingLatch.await();
    assertNotNull(DBOS.workflowId());
    return DBOS.workflowId();
  }

  @Override
  @Workflow
  public String normalWorkflow() {
    assertNotNull(DBOS.workflowId());
    return DBOS.workflowId();
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class PartitionedQueuesTest {
  private static DBOSConfig dbosConfig;
  private static DataSource dataSource;
  private static final String dbUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys";
  private static final String dbUser = "postgres";
  private static final String dbPassword = System.getenv("PGPASSWORD");

  @BeforeAll
  static void onetimeSetup() throws Exception {
    dbosConfig = DBOSConfig.defaultsFromEnv("systemdbtest").withDatabaseUrl(dbUrl);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    dataSource = SystemDatabase.createDataSource(dbosConfig);

    DBOS.reinitialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  public void testResumingQueuedPartitionedWorkflows() throws Exception {
    Queue queue = new Queue("testQueue").withConcurrency(1).withPartitionedEnabled(true);
    DBOS.registerQueue(queue);

    var impl = new ResumingTestServiceImpl();
    var proxy = DBOS.registerWorkflows(ResumingTestService.class, impl);
    DBOS.launch();

    var options = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("key");
    var wfid = UUID.randomUUID().toString();

    // Enqueue a blocked workflow and two regular workflows on a queue with concurrency 1
    var blockedHandle = DBOS.startWorkflow(() -> proxy.stuckWorkflow(), options);
    var regHandle1 =
        DBOS.startWorkflow(() -> proxy.regularWorkflow(), options.withWorkflowId(wfid));
    var regHandle2 = DBOS.startWorkflow(() -> proxy.regularWorkflow(), options);

    // Verify that the blocked workflow starts and is PENDING while the regular workflows remain
    // ENQUEUED.
    impl.startLatch.await();
    assertEquals(WorkflowState.PENDING.toString(), blockedHandle.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED.toString(), regHandle1.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED.toString(), regHandle2.getStatus().status());

    // Resume a regular workflow. Verify it completes.
    DBOS.resumeWorkflow(wfid);
    assertEquals(42, regHandle1.getResult());
    assertEquals(WorkflowState.SUCCESS.toString(), regHandle1.getStatus().status());

    // Complete the blocked workflow. Verify the second regular workflow also completes.
    impl.blockingLatch.countDown();
    assertEquals(13, blockedHandle.getResult());
    assertEquals(42, regHandle2.getResult());

    assertTrue(DBUtils.queueEntriesCleanedUp(dataSource));
  }

  @Test
  public void testQueuePartitions() throws Exception {
    Queue queue = new Queue("testQueue").withWorkerConcurrency(1).withPartitionedEnabled(true);
    Queue partitionlessQueue = new Queue("partitionless-queue");
    DBOS.registerQueues(queue, partitionlessQueue);

    var impl = new PartitionsTestServiceImpl();
    var proxy = DBOS.registerWorkflows(PartitionsTestService.class, impl);
    DBOS.launch();

    var blockedPartitionKey = "blocked";
    var normalPartitionKey = "normal";

    // Enqueue a blocked workflow and a normal workflow on
    // the blocked partition. Verify the blocked workflow starts
    // but the normal workflow is stuck behind it.
    var options =
        new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey(blockedPartitionKey);
    var blockedBlockedHandle = DBOS.startWorkflow(() -> proxy.blockedWorkflow(), options);
    var blockedNormalHandle = DBOS.startWorkflow(() -> proxy.normalWorkflow(), options);

    impl.waitingLatch.await();
    assertEquals(WorkflowState.PENDING.toString(), blockedBlockedHandle.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED.toString(), blockedNormalHandle.getStatus().status());
    assertEquals(blockedPartitionKey, blockedBlockedHandle.getStatus().queuePartitionKey());
    assertEquals(blockedPartitionKey, blockedNormalHandle.getStatus().queuePartitionKey());

    // Enqueue a normal workflow on the other partition and verify it runs normally
    var normalHandle =
        DBOS.startWorkflow(
            () -> proxy.normalWorkflow(), options.withQueuePartitionKey(normalPartitionKey));
    assertEquals(normalHandle.workflowId(), normalHandle.getResult());

    // Unblock the blocked partition and verify its workflows complete
    impl.blockingLatch.countDown();
    assertEquals(blockedBlockedHandle.workflowId(), blockedBlockedHandle.getResult());
    assertEquals(blockedNormalHandle.workflowId(), blockedNormalHandle.getResult());

    try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
      var className = "dev.dbos.transact.queue.PartitionsTestServiceImpl";
      var wfName = "normalWorkflow";
      var nqOptions =
          new DBOSClient.EnqueueOptions(className, wfName, queue.name())
              .withQueuePartitionKey(blockedPartitionKey);
      var clientHandle = client.enqueueWorkflow(nqOptions, null);
      assertEquals(clientHandle.workflowId(), clientHandle.getResult());
    }

    // You can only enqueue on a partitioned queue with a partition key
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DBOS.startWorkflow(
                () -> proxy.normalWorkflow(), new StartWorkflowOptions().withQueue(queue)));

    // Deduplication is not supported for partitioned queues
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DBOS.startWorkflow(
                () -> proxy.normalWorkflow(), options.withDeduplicationId("dedupe")));

    // You can only enqueue with a partition key on a partitioned queue
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DBOS.startWorkflow(
                () -> proxy.normalWorkflow(),
                new StartWorkflowOptions()
                    .withQueue(partitionlessQueue)
                    .withQueuePartitionKey("test")));

    assertTrue(DBUtils.queueEntriesCleanedUp(dataSource));
  }

  @Test
  public void testPartitionKeyWithoutQueue() throws Exception {
    var impl = new PartitionsTestServiceImpl();
    var proxy = DBOS.registerWorkflows(PartitionsTestService.class, impl);
    DBOS.launch();

    var options = new StartWorkflowOptions().withQueuePartitionKey("partition-1");
    assertThrows(
        IllegalArgumentException.class,
        () -> DBOS.startWorkflow(() -> proxy.normalWorkflow(), options));
  }

  @Test
  public void testPartitionKeyOnNonPartitionedQueue() throws Exception {
    var queue = new Queue("non-partitioned-queue");
    DBOS.registerQueue(queue);
    var impl = new PartitionsTestServiceImpl();
    var proxy = DBOS.registerWorkflows(PartitionsTestService.class, impl);
    DBOS.launch();

    var options = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("partition-1");
    assertThrows(
        IllegalArgumentException.class,
        () -> DBOS.startWorkflow(() -> proxy.normalWorkflow(), options));
  }

  @Test
  public void testPartitionedQueueWithoutPartitionKey() throws Exception {
    var queue = new Queue("partitioned-queue").withPartitionedEnabled(true);
    DBOS.registerQueue(queue);
    var impl = new PartitionsTestServiceImpl();
    var proxy = DBOS.registerWorkflows(PartitionsTestService.class, impl);
    DBOS.launch();

    var options = new StartWorkflowOptions().withQueue(queue);
    assertThrows(
        IllegalArgumentException.class,
        () -> DBOS.startWorkflow(() -> proxy.normalWorkflow(), options));
  }

  @Test
  public void testPartitionKeyWithDeduplicationID() throws Exception {
    var queue = new Queue("partitioned-queue").withPartitionedEnabled(true);
    DBOS.registerQueue(queue);
    var impl = new PartitionsTestServiceImpl();
    var proxy = DBOS.registerWorkflows(PartitionsTestService.class, impl);
    DBOS.launch();

    var options =
        new StartWorkflowOptions()
            .withQueue(queue)
            .withQueuePartitionKey("partition-1")
            .withDeduplicationId("dedupe");
    assertThrows(
        IllegalArgumentException.class,
        () -> DBOS.startWorkflow(() -> proxy.normalWorkflow(), options));
  }
}
