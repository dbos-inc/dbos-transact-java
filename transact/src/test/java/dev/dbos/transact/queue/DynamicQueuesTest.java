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

    // The supervisor polls every 5s; wait for it to discover and start a listener.
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
}
