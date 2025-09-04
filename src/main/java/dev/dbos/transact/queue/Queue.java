package dev.dbos.transact.queue;

import java.util.Objects;

public class Queue {
    private final String name;
    private final int concurrency;
    private final int workerConcurrency;
    private RateLimit rateLimit;
    private final boolean priorityEnabled;

    private Queue(String name, int concurrency, int workerConcurrency, RateLimit limit,
            boolean priorityEnabled) {
        this.name = name;
        this.concurrency = concurrency;
        this.workerConcurrency = workerConcurrency;
        this.rateLimit = limit;
        this.priorityEnabled = priorityEnabled;
    }

    public static Queue createQueue(String name, int concurrency, int workerConcurrency,
            RateLimit limit, boolean priorityEnabled) {

        if (workerConcurrency > concurrency) {
            throw new IllegalArgumentException(
                    "worker_concurrency must be less than or equal to concurrency for queue '"
                            + name + "'");
        }

        return new Queue(name, concurrency, workerConcurrency, limit, priorityEnabled);
    }

    public String getName() {
        return name;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public Integer getWorkerConcurrency() {
        return workerConcurrency;
    }

    public RateLimit getRateLimit() {
        return rateLimit;
    }

    public boolean isPriorityEnabled() {
        return priorityEnabled;
    }

    public boolean hasLimiter() {
        return rateLimit != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Queue that = (Queue) o;
        return priorityEnabled == that.priorityEnabled && Objects.equals(name, that.name)
                && Objects.equals(concurrency, that.concurrency)
                && Objects.equals(workerConcurrency, that.workerConcurrency)
                && Objects.equals(rateLimit, that.rateLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name,
                concurrency,
                workerConcurrency,
                rateLimit.getLimit(),
                rateLimit.getPeriod(),
                priorityEnabled);
    }

    @Override
    public String toString() {
        return String.format("WorkflowQueue{name='%s', concurrency=%d, workerConcurrency=%d, limit=%d, period=%d, priorityEnabled=%b}",
            name, concurrency, workerConcurrency,
            rateLimit != null ? rateLimit.getLimit() : null,
            rateLimit != null ? rateLimit.getPeriod() : null,
            priorityEnabled);
    }
}
