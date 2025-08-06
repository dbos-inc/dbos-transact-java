package dev.dbos.transact.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueRegistry {
    private final ConcurrentHashMap<String, Queue> registry = new ConcurrentHashMap<>();

    Logger logger = LoggerFactory.getLogger(QueueRegistry.class);

    public void register(String queueName, Queue queue) {
        if (registry.containsKey(queueName)) {
            logger.warn(String.format("Queue %s has already been registered.", queueName));
        }

        registry.put(queueName, queue);
    }

    public Queue get(String queueName) {
        return registry.get(queueName);
    }

    public List<Queue> getAllQueuesSnapshot() {
        return new ArrayList<>(registry.values());
    }
}
