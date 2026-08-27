package dev.dbos.transact.workflow;

import java.time.Instant;

import org.jspecify.annotations.Nullable;

public record VersionInfo(
    String versionId,
    String versionName,
    Instant versionTimestamp,
    Instant createdAt,
    /** The application that registered this version, or null if the row is unclaimed. */
    @Nullable String applicationName) {}
