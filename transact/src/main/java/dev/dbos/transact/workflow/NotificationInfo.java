package dev.dbos.transact.workflow;

public record NotificationInfo(
    String topic, Object message, long createdAtEpochMs, boolean consumed) {}
