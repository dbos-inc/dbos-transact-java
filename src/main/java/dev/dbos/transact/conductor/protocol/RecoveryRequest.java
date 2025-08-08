package dev.dbos.transact.conductor.protocol;

import java.util.List;

public class RecoveryRequest extends BaseMessage {
    public List<String> executor_ids;
}
