package dev.dbos.transact.database.signal;

public sealed interface SignalKey
    permits SignalKey.Cancellation,
        SignalKey.Event,
        SignalKey.Message,
        SignalKey.Shutdown,
        SignalKey.Stream {

  enum WakeReason {
    MESSAGE,
    EVENT,
    STREAM,
    CANCELLED,
    SHUTDOWN,
    TIMEOUT
  }

  WakeReason wakeReason();

  record Cancellation(String workflowId) implements SignalKey {
    public WakeReason wakeReason() {
      return WakeReason.CANCELLED;
    }
  }

  record Event(String workflowId, String key) implements SignalKey {
    public WakeReason wakeReason() {
      return WakeReason.EVENT;
    }
  }

  record Message(String workflowId, String topic) implements SignalKey {
    public WakeReason wakeReason() {
      return WakeReason.MESSAGE;
    }
  }

  record Shutdown() implements SignalKey {
    public WakeReason wakeReason() {
      return WakeReason.SHUTDOWN;
    }
  }

  record Stream(String workflowId, String key) implements SignalKey {
    public WakeReason wakeReason() {
      return WakeReason.STREAM;
    }
  }
}
