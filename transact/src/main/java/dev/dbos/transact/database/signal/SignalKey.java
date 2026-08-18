package dev.dbos.transact.database.signal;

/**
 * Identifies one thing a caller can wait for, and everything about how a wake-up for it travels.
 *
 * <p>A wake-up reaches a waiter two ways: directly, when the writer is in this process, and over a
 * LISTEN/NOTIFY channel when it is not. Both have to name the same thing or a waiter would be woken
 * by one and not the other, so the channel, the notification payload and the signal-map key are all
 * defined here rather than at the two ends.
 */
public sealed interface SignalKey permits SignalKey.Event, SignalKey.Message, SignalKey.Stream {

  String NOTIFICATIONS_CHANNEL = "dbos_notifications_channel";
  String WORKFLOW_EVENTS_CHANNEL = "dbos_workflow_events_channel";
  String STREAMS_CHANNEL = "dbos_streams_channel";

  /** The LISTEN/NOTIFY channel a wake-up for this key travels on. */
  String channel();

  /** The NOTIFY payload naming this key among the others on its channel. */
  String payload();

  /** The signal-map key: the payload, qualified by which channel it came from. */
  @Override
  String toString();

  /**
   * The signal-map key a notification on {@code channel} carrying {@code payload} refers to.
   *
   * <p>Deliberately not a parse back into a typed key: a workflow ID or topic may itself contain
   * the separator, so the payload cannot be split apart again. Only the qualified key is needed.
   *
   * @throws IllegalArgumentException if the channel is not one of ours
   */
  static String signalFor(String channel, String payload) {
    return switch (channel) {
      case NOTIFICATIONS_CHANNEL -> Message.PREFIX + payload;
      case WORKFLOW_EVENTS_CHANNEL -> Event.PREFIX + payload;
      case STREAMS_CHANNEL -> Stream.PREFIX + payload;
      default -> throw new IllegalArgumentException("Unknown NOTIFY channel: " + channel);
    };
  }

  private static String join(String workflowId, String name) {
    return workflowId + "::" + name;
  }

  /** A value published by {@code setEvent}, awaited by {@code getEvent}. */
  record Event(String workflowId, String key) implements SignalKey {
    static final String PREFIX = "e::";

    @Override
    public String channel() {
      return WORKFLOW_EVENTS_CHANNEL;
    }

    @Override
    public String payload() {
      return join(workflowId, key);
    }

    @Override
    public String toString() {
      return PREFIX + payload();
    }
  }

  /** A message sent to a workflow, awaited by {@code recv}. */
  record Message(String workflowId, String topic) implements SignalKey {
    static final String PREFIX = "m::";

    @Override
    public String channel() {
      return NOTIFICATIONS_CHANNEL;
    }

    @Override
    public String payload() {
      return join(workflowId, topic);
    }

    @Override
    public String toString() {
      return PREFIX + payload();
    }
  }

  /** A value appended to a durable stream, awaited by a stream read. */
  record Stream(String workflowId, String key) implements SignalKey {
    static final String PREFIX = "s::";

    @Override
    public String channel() {
      return STREAMS_CHANNEL;
    }

    @Override
    public String payload() {
      return join(workflowId, key);
    }

    @Override
    public String toString() {
      return PREFIX + payload();
    }
  }
}
