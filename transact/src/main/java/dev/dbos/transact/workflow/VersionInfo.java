package dev.dbos.transact.workflow;

public record VersionInfo(
    String versionId,
    String versionName,
    long versionTimestamp,
    long createdAt) {}
