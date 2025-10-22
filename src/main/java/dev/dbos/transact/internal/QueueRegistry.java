package dev.dbos.transact.internal;

import dev.dbos.transact.workflow.Queue;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueRegistry {
  private final ConcurrentHashMap<String, Queue> registry = new ConcurrentHashMap<>();

  private static final Logger logger = LoggerFactory.getLogger(QueueRegistry.class);

  public void register(Queue queue) {
    if (queue.concurrency() != null
        && queue.workerConcurrency() != null
        && queue.workerConcurrency() > queue.concurrency()) {
      throw new IllegalArgumentException(
          String.format(
              "workerConcurrency must be less than or equal to concurrency for queue %s",
              queue.name()));
    }

    var queueName = queue.name();
    var previous = registry.putIfAbsent(queueName, queue);

    if (previous != null) {
      logger.warn("Queue {} has already been registered.", queueName);
    }
  }

  public Queue get(String queueName) {
    return registry.get(queueName);
  }

  public void clear() {
    registry.clear();
  }

  public List<Queue> getSnapshot() {
    return List.copyOf(registry.values());
  }
}
