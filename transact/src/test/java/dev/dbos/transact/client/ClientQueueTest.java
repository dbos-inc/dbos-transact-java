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
      client.registerQueue("cq-find", QueueOptions.setConcurrency(7));

      var q = client.findQueue("cq-find").orElseThrow();
      assertEquals("cq-find", q.name());
      assertEquals(7, q.concurrency());

      assertTrue(client.findQueue("cq-does-not-exist").isEmpty());
    }
  }

  @Test
  public void testClientListQueues() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-list-1", QueueOptions.setConcurrency(1));
      client.registerQueue("cq-list-2", QueueOptions.setConcurrency(2));
      client.registerQueue("cq-list-3", QueueOptions.empty());

      var names = client.listQueues().stream().map(Queue::name).toList();
      assertTrue(names.contains("cq-list-1"));
      assertTrue(names.contains("cq-list-2"));
      assertTrue(names.contains("cq-list-3"));
    }
  }

  @Test
  public void testClientDeleteQueue() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-del", QueueOptions.empty());
      assertTrue(client.findQueue("cq-del").isPresent());

      assertTrue(client.deleteQueue("cq-del"));
      assertTrue(client.findQueue("cq-del").isEmpty());

      assertFalse(client.deleteQueue("cq-never-existed"));
    }
  }

  @Test
  public void testClientUpdateQueue() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-update", QueueOptions.setConcurrency(5));
      assertEquals(5, client.findQueue("cq-update").orElseThrow().concurrency());

      client.updateQueue("cq-update", QueueOptions.setConcurrency(10));
      assertEquals(10, client.findQueue("cq-update").orElseThrow().concurrency());
    }
  }

  @Test
  public void testClientNeverUpdate() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-never", QueueOptions.setConcurrency(5));
      client.registerQueue(
          "cq-never", QueueOptions.setConcurrency(99), QueueConflictResolution.NEVER_UPDATE);

      assertEquals(5, client.findQueue("cq-never").orElseThrow().concurrency());
    }
  }

  @Test
  public void testClientAlwaysUpdate() {
    try (var client = pgContainer.dbosClient()) {
      client.registerQueue("cq-always", QueueOptions.setConcurrency(5));
      client.registerQueue(
          "cq-always", QueueOptions.setConcurrency(99), QueueConflictResolution.ALWAYS_UPDATE);

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
                  QueueOptions.empty(),
                  QueueConflictResolution.UPDATE_IF_LATEST_VERSION));
    }
  }

  @Test
  public void testClientRejectsInternalQueueName() {
    try (var client = pgContainer.dbosClient()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> client.registerQueue("_dbos_internal_queue", QueueOptions.empty()));
    }
  }
}
