package dev.dbos.transact.internal;

import dev.dbos.transact.queue.Queue;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueRegistry {
  private final ConcurrentHashMap<String, Queue> registry = new ConcurrentHashMap<>();

  Logger logger = LoggerFactory.getLogger(QueueRegistry.class);

  public void register(Queue queue) {
    var queueName = queue.getName();
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
