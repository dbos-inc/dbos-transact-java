package dev.dbos.transact.database.signal;

public sealed interface SignalKey permits SignalKey.Event, SignalKey.Message, SignalKey.Stream {

  record Event(String workflowId, String key) implements SignalKey {
    public String toString() {
      return "e::%s::%s".formatted(workflowId, key);
    }
  }

  record Message(String workflowId, String topic) implements SignalKey {
    public String toString() {
      return "m::%s::%s".formatted(workflowId, topic);
    }
  }

  record Stream(String workflowId, String key) implements SignalKey {
    public String toString() {
      return "s::%s::%s".formatted(workflowId, key);
    }
  }
}
