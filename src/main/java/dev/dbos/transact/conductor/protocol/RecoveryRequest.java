package dev.dbos.transact.conductor.protocol;

import java.util.List;

public class RecoveryRequest extends BaseMessage {
    public List<String> executor_ids;

    public RecoveryRequest() {
    }

    public RecoveryRequest(String requestId, List<String> executorIds) {
        this.type = MessageType.RECOVERY.getValue();
        this.request_id = requestId;
        this.executor_ids = executorIds;
    }

}
