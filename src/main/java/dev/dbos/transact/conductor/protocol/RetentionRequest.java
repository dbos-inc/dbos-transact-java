package dev.dbos.transact.conductor.protocol;

public class RetentionRequest extends BaseMessage {
    public RetentionBody body;

    public static class RetentionBody {
        public Long gc_cutoff_epoch_ms;
        public Long gc_rows_threshold;
        public Long timeout_cutoff_epoch_ms;
    }
}
