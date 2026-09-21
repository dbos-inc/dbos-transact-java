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

    // The same refusal QueuesDAO makes for dynamic registration, which this path never reaches:
    // a static queue is polled from memory and never written to the database. Nothing here
    // enforces a per-partition limit yet -- every consumer still branches on the stored
    // partitioningEnabled flag -- so a queue registered with one would be polled as though it
    // were unpartitioned, and would reject the partition keys the caller then tried to enqueue
    // with. Deleted by #507's dequeue slice, which is what makes the limits mean something.
    if (queue.hasPartitionLimits()) {
      throw new UnsupportedOperationException(
          "Per-partition queue limits are not enforced yet; see dbos-transact-java#507");
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
