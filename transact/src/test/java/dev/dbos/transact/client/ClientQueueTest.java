package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueConflictResolution;
import dev.dbos.transact.workflow.QueueOptions;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;

public class ClientQueueTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @Test
  public void testClientRegisterAndFindQueue() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-find", new QueueOptions().withConcurrency(7));

      var q = client.findQueue("cq-find").orElseThrow();
      assertEquals("cq-find", q.name());
      assertEquals(7, q.concurrency());

      assertTrue(client.findQueue("cq-does-not-exist").isEmpty());
    }
  }

  @Test
  public void testClientListQueues() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-list-1", new QueueOptions().withConcurrency(1));
      client.registerQueue("cq-list-2", new QueueOptions().withConcurrency(2));
      client.registerQueue("cq-list-3", new QueueOptions());

      var names = client.listQueues().stream().map(Queue::name).toList();
      assertTrue(names.contains("cq-list-1"));
      assertTrue(names.contains("cq-list-2"));
      assertTrue(names.contains("cq-list-3"));
    }
  }

  @Test
  public void testClientDeleteQueue() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-del", new QueueOptions());
      assertTrue(client.findQueue("cq-del").isPresent());

      assertTrue(client.deleteQueue("cq-del"));
      assertTrue(client.findQueue("cq-del").isEmpty());

      assertFalse(client.deleteQueue("cq-never-existed"));
    }
  }

  @Test
  public void testClientUpdateQueue() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-update", new QueueOptions().withConcurrency(5));
      assertEquals(5, client.findQueue("cq-update").orElseThrow().concurrency());

      client.updateQueue("cq-update", new QueueOptions().withConcurrency(10));
      assertEquals(10, client.findQueue("cq-update").orElseThrow().concurrency());
    }
  }

  @Test
  public void testClientZeroConcurrencyPausesAndNegativeIsRefused() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue(
          "cq-paused", new QueueOptions().withConcurrency(0).withWorkerConcurrency(0));
      var paused = client.findQueue("cq-paused").orElseThrow();
      assertEquals(0, paused.concurrency());
      assertEquals(0, paused.workerConcurrency());

      assertThrows(
          IllegalArgumentException.class,
          () -> client.registerQueue("cq-negative", new QueueOptions().withConcurrency(-1)));
      assertTrue(client.findQueue("cq-negative").isEmpty());
      assertThrows(
          IllegalArgumentException.class,
          () -> client.updateQueue("cq-paused", new QueueOptions().withWorkerConcurrency(-1)));

      client.registerQueue("cq-running", new QueueOptions().withConcurrency(5));
      client.updateQueue("cq-running", new QueueOptions().withConcurrency(0));
      assertEquals(0, client.findQueue("cq-running").orElseThrow().concurrency());
      client.updateQueue("cq-running", new QueueOptions().withConcurrency(3));
      assertEquals(3, client.findQueue("cq-running").orElseThrow().concurrency());
    }
  }

  @Test
  @SuppressWarnings("removal") // sets the deprecated partitionQueue flag to see it refused
  public void testClientRefusesPartitionQueueAlongsidePartitionLimit() {
    try (var client = pgContainer.dbosClient()) {
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  client.registerQueue(
                      "cq-flag-and-limit",
                      new QueueOptions().andPartitionQueue(true).withPartitionConcurrency(2)));
      assertTrue(ex.getMessage().contains("set only one of them"), ex.getMessage());
      assertTrue(client.findQueue("cq-flag-and-limit").isEmpty());

      client.registerQueue("cq-flag-update", new QueueOptions().withConcurrency(4));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              client.updateQueue(
                  "cq-flag-update",
                  new QueueOptions().andPartitionQueue(true).withPartitionWorkerConcurrency(1)));
      assertFalse(client.findQueue("cq-flag-update").orElseThrow().isPartitioned());
    }
  }

  @Test
  public void testClientNeverUpdate() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-never", new QueueOptions().withConcurrency(5));
      client.registerQueue(
          "cq-never", new QueueOptions().withConcurrency(99), QueueConflictResolution.NEVER_UPDATE);

      assertEquals(5, client.findQueue("cq-never").orElseThrow().concurrency());
    }
  }

  @Test
  public void testClientAlwaysUpdate() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-always", new QueueOptions().withConcurrency(5));
      client.registerQueue(
          "cq-always",
          new QueueOptions().withConcurrency(99),
          QueueConflictResolution.ALWAYS_UPDATE);

      assertEquals(99, client.findQueue("cq-always").orElseThrow().concurrency());
    }
  }

  @Test
  public void testClientRejectsUpdateIfLatestVersion() {
    try (var client = pgContainer.dbosClient()) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              client.registerQueue(
                  "cq-version",
                  new QueueOptions(),
                  QueueConflictResolution.UPDATE_IF_LATEST_VERSION));
    }
  }

  @Test
  public void testClientRejectsInternalQueueName() {
    try (var client = pgContainer.dbosClient()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> client.registerQueue("_dbos_internal_queue", new QueueOptions()));
    }
  }
}
