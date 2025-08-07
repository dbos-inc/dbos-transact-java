package dev.dbos.transact.queue;

public class QueueMetadata {
    public String name;
    public Integer concurrency;
    public Integer workerConcurrency;
    public RateLimitMetadata rateLimit;
    public Boolean priorityEnabled;

    public QueueMetadata(Queue queue) {
        this.name = queue.getName();
        this.concurrency = queue.getConcurrency();
        this.workerConcurrency = queue.getWorkerConcurrency();
        if (queue.getRateLimit() != null) {
            this.rateLimit = new RateLimitMetadata(queue.getRateLimit());
        } else {
            this.rateLimit = null;
        }
        this.priorityEnabled = queue.isPriorityEnabled();
    }

    public static class RateLimitMetadata {
        public Integer limit;
        public Double period;

        public RateLimitMetadata(dev.dbos.transact.queue.RateLimit rateLimit) {
            this.limit = rateLimit.getLimit();
            this.period = rateLimit.getPeriod();
        }
    }
}
