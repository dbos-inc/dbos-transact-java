package dev.dbos.transact.config;

import dev.dbos.transact.Constants;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.workflow.Queue;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Immutable configuration for a DBOS application instance.
 *
 * <p>Use {@link #defaults(String)} or {@link #defaultsFromEnv(String)} to obtain a base
 * configuration, then chain {@code with*} methods to set individual options:
 *
 * <pre>{@code
 * DBOSConfig config = DBOSConfig.defaults("my-app")
 *     .withDatabaseUrl("jdbc:postgresql://localhost:5432/mydb")
 *     .withMigrate(true);
 * }</pre>
 *
 * @param appName unique name for this application; must not be null or empty
 * @param databaseUrl JDBC URL of the PostgreSQL database; may be {@code null} when a {@link
 *     DataSource} is provided instead
 * @param dbUser PostgreSQL username; may be {@code null} to rely on driver defaults
 * @param dbPassword PostgreSQL password; masked in {@link #toString()}
 * @param dataSource pre-configured {@link DataSource} to use instead of {@code databaseUrl}, {@code
 *     dbUser}, and {@code dbPassword}
 * @param adminServer whether to start the built-in admin HTTP server; deprecated and will be
 *     removed before 1.0 — use DBOS Conductor instead
 * @param adminServerPort port for the built-in admin HTTP server; deprecated and will be removed
 *     before 1.0 — use DBOS Conductor instead
 * @param migrate whether to run database migrations on startup
 * @param conductorKey API key for DBOS Conductor; must not be empty if non-null
 * @param conductorDomain custom hostname for DBOS Conductor; must not be empty if non-null
 * @param conductorExecutorMetadata arbitrary key-value metadata attached to this executor's
 *     registration in Conductor
 * @param appVersion application version string used for versioned workflow routing; must not be
 *     empty if non-null
 * @param executorId stable identifier for this executor instance; must not be empty if non-null
 * @param databaseSchema PostgreSQL schema used for DBOS system tables; {@code null} uses the
 *     default schema
 * @param enablePatching whether to enable workflow patching
 * @param listenQueues names of queues this executor should dequeue work from; an empty set means
 *     listen on all queues
 * @param serializer custom {@link DBOSSerializer} for workflow arguments and return values; {@code
 *     null} uses the default JSON serializer
 * @param schedulerPollingInterval how often the cron scheduler polls for due tasks; {@code null}
 *     uses the system default
 * @param useListenNotify whether to use PostgreSQL {@code LISTEN}/{@code NOTIFY} for real-time
 *     workflow wake-ups instead of polling
 */
public record DBOSConfig(
    @NonNull String appName,
    @Nullable String databaseUrl,
    @Nullable String dbUser,
    @Nullable String dbPassword,
    @Nullable DataSource dataSource,
    boolean adminServer,
    int adminServerPort,
    boolean migrate,
    @Nullable String conductorKey,
    @Nullable String conductorDomain,
    @Nullable Map<String, Object> conductorExecutorMetadata,
    @Nullable String appVersion,
    @Nullable String executorId,
    @Nullable String databaseSchema,
    boolean enablePatching,
    @NonNull Set<String> listenQueues,
    @Nullable DBOSSerializer serializer,
    @Nullable Duration schedulerPollingInterval,
    boolean useListenNotify) {

  public DBOSConfig {
    if (appName == null || appName.isEmpty()) {
      throw new IllegalArgumentException("DBOSConfig.appName must not be null or empty");
    }
    if (conductorKey != null && conductorKey.isEmpty()) {
      throw new IllegalArgumentException("DBOSConfig.conductorKey must not be empty if specified");
    }
    if (conductorDomain != null && conductorDomain.isEmpty()) {
      throw new IllegalArgumentException(
          "DBOSConfig.conductorDomain must not be empty if specified");
    }
    if (appVersion != null && appVersion.isEmpty()) {
      throw new IllegalArgumentException("DBOSConfig.appVersion must not be empty if specified");
    }
    if (executorId != null && executorId.isEmpty()) {
      throw new IllegalArgumentException("DBOSConfig.executorId must not be empty if specified");
    }

    listenQueues = (listenQueues == null) ? Set.of() : Set.copyOf(listenQueues);
  }

  /**
   * @deprecated The built-in admin server will be removed before 1.0. Use DBOS Conductor instead.
   */
  @Deprecated(since = "0.9", forRemoval = true)
  @Override
  public boolean adminServer() {
    return adminServer;
  }

  /**
   * @deprecated The built-in admin server will be removed before 1.0. Use DBOS Conductor instead.
   */
  @Deprecated(since = "0.9", forRemoval = true)
  @Override
  public int adminServerPort() {
    return adminServerPort;
  }

  // Copy constructor
  public DBOSConfig(DBOSConfig other) {
    this(
        other.appName,
        other.databaseUrl,
        other.dbUser,
        other.dbPassword,
        other.dataSource,
        other.adminServer,
        other.adminServerPort,
        other.migrate,
        other.conductorKey,
        other.conductorDomain,
        (other.conductorExecutorMetadata == null
            ? null
            : Map.copyOf(other.conductorExecutorMetadata)),
        other.appVersion,
        other.executorId,
        other.databaseSchema,
        other.enablePatching,
        (other.listenQueues == null ? null : Set.copyOf(other.listenQueues)),
        other.serializer,
        other.schedulerPollingInterval,
        other.useListenNotify);
  }

  /**
   * Returns a {@link DBOSConfig} with sensible defaults for the given application name.
   *
   * <p>Migrations are enabled and {@code LISTEN}/{@code NOTIFY} is used for workflow wake-ups. No
   * database connection details are pre-filled; supply them with {@link #withDatabaseUrl}, {@link
   * #withDbUser}/{@link #withDbPassword}, or {@link #withDataSource}.
   *
   * @param appName unique application name; must not be null or empty
   * @return a default {@link DBOSConfig} for the given name
   */
  public static @NonNull DBOSConfig defaults(@NonNull String appName) {
    return new DBOSConfig(
        appName, null, null, null, null, false, // adminServer
        3001, // adminServerPort
        true, // migrate
        null, null, null, null, null, null, false, null, null, null, true); // useListenNotify
  }

  /**
   * Returns a {@link DBOSConfig} populated from standard environment variables.
   *
   * <p>Reads the following variables:
   *
   * <ul>
   *   <li>{@code DBOS_SYSTEM_JDBC_URL} — JDBC URL for the database
   *   <li>{@code PGUSER} — database username (defaults to {@code "postgres"} if absent)
   *   <li>{@code PGPASSWORD} — database password
   * </ul>
   *
   * @param appName unique application name; must not be null or empty
   * @return a {@link DBOSConfig} seeded from environment variables
   */
  public static @NonNull DBOSConfig defaultsFromEnv(@NonNull String appName) {
    String databaseUrl = System.getenv(Constants.SYSTEM_JDBC_URL_ENV_VAR);
    String dbUser = System.getenv(Constants.POSTGRES_USER_ENV_VAR);
    if (dbUser == null || dbUser.isEmpty()) dbUser = "postgres";
    String dbPassword = System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR);
    return defaults(appName)
        .withDatabaseUrl(databaseUrl)
        .withDbUser(dbUser)
        .withDbPassword(dbPassword);
  }

  /** Returns a copy of this config with {@code appName} set to {@code v}. */
  public @NonNull DBOSConfig withAppName(@NonNull String v) {
    return new DBOSConfig(
        v,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code databaseUrl} set to {@code v}. */
  public @NonNull DBOSConfig withDatabaseUrl(@Nullable String v) {
    return new DBOSConfig(
        appName,
        v,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code dbUser} set to {@code v}. */
  public @NonNull DBOSConfig withDbUser(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        v,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code dbPassword} set to {@code v}. */
  public @NonNull DBOSConfig withDbPassword(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        v,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /**
   * Returns a copy of this config with {@code dataSource} set to {@code v}.
   *
   * <p>When a {@link DataSource} is provided it takes precedence over {@code databaseUrl}, {@code
   * dbUser}, and {@code dbPassword}.
   */
  public @NonNull DBOSConfig withDataSource(@Nullable DataSource v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        v,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /**
   * Returns a copy of this config with {@code adminServer} set to {@code v}.
   *
   * @deprecated The built-in admin server will be removed before 1.0. Use DBOS Conductor instead.
   */
  @Deprecated(since = "0.9", forRemoval = true)
  public @NonNull DBOSConfig withAdminServer(boolean v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        v,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /**
   * Returns a copy of this config with {@code adminServerPort} set to {@code v}.
   *
   * @deprecated The built-in admin server will be removed before 1.0. Use DBOS Conductor instead.
   */
  @Deprecated(since = "0.9", forRemoval = true)
  public @NonNull DBOSConfig withAdminServerPort(int v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        v,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code migrate} set to {@code v}. */
  public @NonNull DBOSConfig withMigrate(boolean v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        v,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code conductorKey} set to {@code v}. */
  public @NonNull DBOSConfig withConductorKey(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        v,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code conductorDomain} set to {@code v}. */
  public @NonNull DBOSConfig withConductorDomain(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        v,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code conductorExecutorMetadata} set to {@code v}. */
  public @NonNull DBOSConfig withConductorExecutorMetadata(@Nullable Map<String, Object> v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        v,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code appVersion} set to {@code v}. */
  public @NonNull DBOSConfig withAppVersion(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        v,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code executorId} set to {@code v}. */
  public @NonNull DBOSConfig withExecutorId(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        v,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code databaseSchema} set to {@code v}. */
  public @NonNull DBOSConfig withDatabaseSchema(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        v,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code enablePatching} set to {@code true}. */
  public @NonNull DBOSConfig withEnablePatching() {
    return this.withEnablePatching(true);
  }

  /** Returns a copy of this config with {@code enablePatching} set to {@code false}. */
  public @NonNull DBOSConfig withDisablePatching() {
    return this.withEnablePatching(false);
  }

  /** Returns a copy of this config with {@code enablePatching} set to {@code v}. */
  public @NonNull DBOSConfig withEnablePatching(boolean v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        v,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /**
   * Returns a copy of this config with the built-in admin server enabled.
   *
   * @deprecated The built-in admin server will be removed before 1.0. Use DBOS Conductor instead.
   */
  @Deprecated(since = "0.9", forRemoval = true)
  public @NonNull DBOSConfig enableAdminServer() {
    return withAdminServer(true);
  }

  /**
   * Returns a copy of this config with the built-in admin server disabled.
   *
   * @deprecated The built-in admin server will be removed before 1.0. Use DBOS Conductor instead.
   */
  @Deprecated(since = "0.9", forRemoval = true)
  public @NonNull DBOSConfig disableAdminServer() {
    return withAdminServer(false);
  }

  /**
   * Returns a copy of this config with {@code queue} added to the set of listened queues. Removes
   * the listen-on-all-queues default if this is the first queue specified.
   *
   * @param queue the queue to add; must not be null
   */
  public @NonNull DBOSConfig withListenQueue(@NonNull Queue queue) {
    return withListenQueues(new String[] {queue.name()});
  }

  /**
   * Returns a copy of this config with {@code queueName} added to the set of listened queues.
   * Removes the listen-on-all-queues default if this is the first queue specified.
   *
   * @param queueName the queue name to add; must not be null
   */
  public @NonNull DBOSConfig withListenQueue(@NonNull String queueName) {
    return withListenQueues(new String[] {queueName});
  }

  /**
   * Returns a copy of this config with each queue in {@code queues} added to the listened set.
   * Removes the listen-on-all-queues default if this is the first queue specified.
   *
   * @param queues the queues to add; {@code null} entries are ignored
   */
  public @NonNull DBOSConfig withListenQueues(@Nullable Queue... queues) {
    var names =
        Arrays.stream(Objects.requireNonNullElseGet(queues, () -> new Queue[0]))
            .map(Queue::name)
            .toList();
    return withListenQueues(names.toArray(String[]::new));
  }

  /**
   * Returns a copy of this config with each name in {@code queueNames} added to the listened set.
   * Removes the listen-on-all-queues default if this is the first queue specified.
   *
   * @param queueNames the queue names to add; {@code null} entries are ignored
   */
  public @NonNull DBOSConfig withListenQueues(@Nullable String... queueNames) {
    var v =
        Stream.concat(
                listenQueues.stream(),
                Arrays.stream(Objects.requireNonNullElseGet(queueNames, () -> new String[0])))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        v,
        serializer,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code serializer} set to {@code v}. */
  public @NonNull DBOSConfig withSerializer(@Nullable DBOSSerializer v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        v,
        schedulerPollingInterval,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code schedulerPollingInterval} set to {@code v}. */
  public @NonNull DBOSConfig withSchedulerPollingInterval(@Nullable Duration v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        v,
        useListenNotify);
  }

  /** Returns a copy of this config with {@code useListenNotify} set to {@code v}. */
  public @NonNull DBOSConfig withUseListenNotify(boolean v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        conductorExecutorMetadata,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues,
        serializer,
        schedulerPollingInterval,
        v);
  }

  // Override toString to mask the DB password
  @Override
  public String toString() {
    return """
        DBOSConfig[appName=%s, databaseUrl=%s, dbUser=%s, dbPassword=***, \
        dataSource=%s, databaseSchema=%s, adminServer=%s, adminServerPort=%d, \
        migrate=%s, conductorKey=%s, conductorDomain=%s, \
        conductorExecutorMetadata=%s, appVersion=%s, executorId=%s, enablePatching=%s, \
        listenQueues=%s, serializer=%s, schedulerPollingInterval=%s, useListenNotify=%s]
        """
        .formatted(
            appName,
            databaseUrl,
            dbUser,
            dataSource,
            databaseSchema,
            adminServer,
            adminServerPort,
            migrate,
            conductorKey,
            conductorDomain,
            conductorExecutorMetadata,
            appVersion,
            executorId,
            enablePatching,
            listenQueues,
            serializer != null ? serializer.name() : null,
            schedulerPollingInterval,
            useListenNotify);
  }
}
