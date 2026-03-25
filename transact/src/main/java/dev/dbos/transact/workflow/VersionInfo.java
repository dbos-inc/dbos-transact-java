package dev.dbos.transact.workflow;

import java.time.Instant;

public record VersionInfo(
    String versionId, String versionName, Instant versionTimestamp, Instant createdAt) {}
