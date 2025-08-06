package dev.dbos.transact.exceptions;

import static dev.dbos.transact.exceptions.ErrorCode.QUEUE_DUPLICATED;

public class DBOSQueueDuplicatedException extends DBOSException {
    private final String workflowUUID;
    private final String queueName;
    private final String deduplicationID;

    public DBOSQueueDuplicatedException(String workflowUUID, String queueName,
            String deduplicationID) {
        super(QUEUE_DUPLICATED.getCode(), String.format(
                "Workflow %s (Queue: %s, Deduplication ID: %s) is already enqueued.",
                workflowUUID, queueName, deduplicationID));
        this.workflowUUID = workflowUUID;
        this.queueName = queueName;
        this.deduplicationID = deduplicationID;
    }

    public String getWorkflowUUID() {
        return workflowUUID;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getDeduplicationID() {
        return deduplicationID;
    }
}
