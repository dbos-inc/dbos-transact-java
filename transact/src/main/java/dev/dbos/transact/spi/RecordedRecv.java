package dev.dbos.transact.spi;

import org.jspecify.annotations.Nullable;

/**
 * What a {@code recv} recorded when it first ran.
 *
 * @param message the message it returned, or {@code null} if it returned none: its timeout passed,
 *     or a peer took the message it saw
 */
public record RecordedRecv(@Nullable Object message) {}
