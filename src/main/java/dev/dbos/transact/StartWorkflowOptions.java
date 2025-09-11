package dev.dbos.transact;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalInt;

import dev.dbos.transact.queue.Queue;

public record StartWorkflowOptions(
    String workflowId,
    Duration timeout,
    Queue queue,
    String deduplicationId,
    OptionalInt priority
) {

    public static StartWorkflowOptions fromWorkflowId(String workflowId) {
        return builder(workflowId).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(String workflowId) {
        return builder().workflowId(workflowId);
    }

    public static class Builder {
        private String workflowId;
        private Duration timeout;
        private Queue queue;
        private String deduplicationId;
        private OptionalInt priority = OptionalInt.empty();

        public Builder workflowId(String workflowId) {
            this.workflowId = Objects.requireNonNull(workflowId);
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = Objects.requireNonNull(timeout);
            return this;
        }

        public Builder queue(Queue queue) {
            this.queue = Objects.requireNonNull(queue);
            return this;
        }

        public Builder deduplicationId(String deduplicationId) {
            this.deduplicationId = Objects.requireNonNull(deduplicationId);
            return this;
        }

        public Builder priority(int priority) {
            this.priority = OptionalInt.of(priority);
            return this;
        }

        public StartWorkflowOptions build() {
            return new StartWorkflowOptions(
                workflowId,
                timeout,
                queue,
                deduplicationId,
                priority
            );
        }
    }

}