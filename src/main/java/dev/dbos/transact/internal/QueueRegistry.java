package dev.dbos.transact.internal;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.dbos.transact.workflow.Queue;

public class QueueRegistry {
  private final ConcurrentHashMap<String, Queue> registry = new ConcurrentHashMap<>();

  private static final Logger logger = LoggerFactory.getLogger(QueueRegistry.class);

  public void register(Queue queue) {
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
