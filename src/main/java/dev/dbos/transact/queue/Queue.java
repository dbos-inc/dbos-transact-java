package dev.dbos.transact.queue;

import java.util.Objects;

public class Queue {
    private final String name;
    private final int concurrency;
    private final Integer workerConcurrency;
    private final Integer limit;
    private final Double period;
    private final boolean priorityEnabled;

    private Queue(String name, int concurrency, int workerConcurrency, int limit, Double period, boolean priorityEnabled) {
        this.name = name;
        this.concurrency = concurrency;
        this.workerConcurrency = workerConcurrency;
        this.limit = limit;
        this.period = period;
        this.priorityEnabled = priorityEnabled;


    }

    public static Queue createQueue(String name, int concurrency, int workerConcurrency, int limit, Double period, boolean priorityEnabled) {

        if (workerConcurrency > concurrency) {
            throw new IllegalArgumentException(
                    "worker_concurrency must be less than or equal to concurrency for queue '" + name + "'"
            );
        }

        return new Queue(name, concurrency, workerConcurrency, limit, period, priorityEnabled) ;

    }

    public String getName() { return name; }
    public Integer getConcurrency() { return concurrency; }
    public Integer getWorkerConcurrency() { return workerConcurrency; }
    public Integer getLimit() { return limit; }
    public Double getPeriod() { return period; }
    public boolean isPriorityEnabled() { return priorityEnabled; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Queue that = (Queue) o;
        return priorityEnabled == that.priorityEnabled &&
                Objects.equals(name, that.name) &&
                Objects.equals(concurrency, that.concurrency) &&
                Objects.equals(workerConcurrency, that.workerConcurrency) &&
                Objects.equals(limit, that.limit) &&
                Objects.equals(period, that.period);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, concurrency, workerConcurrency, limit, period, priorityEnabled);
    }

    @Override
    public String toString() {
        return "WorkflowQueue{" +
                "name='" + name + '\'' +
                ", concurrency=" + concurrency +
                ", workerConcurrency=" + workerConcurrency +
                ", limit=" + limit +
                ", period=" + period +
                ", priorityEnabled=" + priorityEnabled +
                '}';
    }

    /* public static class Builder {

        private final String name;

        private Integer concurrency = null;
        private Integer workerConcurrency = null;
        private Integer limit = null;
        private Double period = null;
        private boolean priorityEnabled = false;


        public Builder(String name) {
            this.name = name;
        }

        public Builder concurrency(Integer concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder workerConcurrency(Integer workerConcurrency) {
            this.workerConcurrency = workerConcurrency;
            return this;
        }

        public Builder limit(Integer limit) {
            this.limit = limit;
            return this;
        }

        public Builder period(Double period) {
            this.period = period;
            return this;
        }

        public Builder priorityEnabled(boolean priorityEnabled) {
            this.priorityEnabled = priorityEnabled;
            return this;
        }


        public WorkflowQueue build() {
            return new WorkflowQueue(this);
        }
    } */
}
