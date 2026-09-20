package dev.dbos.transact.internal;

import dev.dbos.transact.Constants;
import dev.dbos.transact.workflow.Queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// The pre-launch static registry itself, which DBOS.registerQueue(Queue) is the entry point to.
// Both are deprecated for removal together; until then this class is the one place that still
// builds a Queue by hand.
@SuppressWarnings("removal")
public class QueueRegistry {
  private final ConcurrentHashMap<String, Queue> registry = new ConcurrentHashMap<>();
  private final Queue internalQueue = new Queue(Constants.DBOS_INTERNAL_QUEUE);

  private static final Logger logger = LoggerFactory.getLogger(QueueRegistry.class);

  public void register(Queue queue) {
    if (queue.name().equals(Constants.DBOS_INTERNAL_QUEUE)) {
      throw new IllegalArgumentException(
          String.format("%s is a reserved queue name", Constants.DBOS_INTERNAL_QUEUE));
    }

    if (queue.concurrency() != null
        && queue.workerConcurrency() != null
        && queue.workerConcurrency() > queue.concurrency()) {
      throw new IllegalArgumentException(
          String.format(
              "workerConcurrency must be less than or equal to concurrency for queue %s",
              queue.name()));
    }

    // Per-partition limits are a database-backed feature, and permanently so. An in-memory queue
    // is polled from this map and never written, so the partitioning flag other SDKs read is
    // never stored for it, and its limits would be invisible to every other executor -- which is
    // the opposite of what a limit shared across partitions means. Go has no in-memory queues at
    // all, and this path is itself deprecated for removal, so the feature simply does not extend
    // to it: register the queue after launch to use these limits.
    if (queue.hasPartitionLimits()) {
      throw new IllegalArgumentException(
          String.format(
              "cannot set per-partition limits on in-memory queue %s: they are supported only on"
                  + " database-backed queues, registered with registerQueue(String, QueueOptions)"
                  + " after launch",
              queue.name()));
    }

    var queueName = queue.name();
    var previous = registry.putIfAbsent(queueName, queue);

    if (previous != null) {
      logger.warn("Queue {} has already been registered.", queueName);
    }
  }

  public Queue get(String queueName) {
    if (queueName.equals(Constants.DBOS_INTERNAL_QUEUE)) {
      return internalQueue;
    }
    return registry.get(queueName);
  }

  public void clear() {
    registry.clear();
  }

  public List<Queue> getSnapshot() {
    var queues = new ArrayList<>(registry.values());
    queues.add(internalQueue);
    return List.copyOf(queues);
  }
}
