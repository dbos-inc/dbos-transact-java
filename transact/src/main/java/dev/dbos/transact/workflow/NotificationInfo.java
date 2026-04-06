package dev.dbos.transact.workflow;

public record NotificationInfo(
    String topic, String message, long createdAtEpochMs, boolean consumed) {}
