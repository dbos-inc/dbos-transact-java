package dev.dbos.transact.conductor;

import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * How a Conductor client connects.
 *
 * @param apiKey the Conductor API key
 * @param appName the application name Conductor addresses this executor by; {@code null} means the
 *     executor's application name
 * @param domain the Conductor host or URL; {@code null} means DBOS Conductor's public endpoint
 */
public record ConductorOptions(String apiKey, @Nullable String appName, @Nullable String domain) {
  public ConductorOptions {
    Objects.requireNonNull(apiKey, "apiKey must not be null");
  }
}
