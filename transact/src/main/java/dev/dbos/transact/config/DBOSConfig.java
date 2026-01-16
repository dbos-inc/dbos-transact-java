package dev.dbos.transact.config;

import dev.dbos.transact.Constants;
import dev.dbos.transact.workflow.Queue;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

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
    @Nullable String appVersion,
    @Nullable String executorId,
    @Nullable String databaseSchema,
    boolean enablePatching,
    @NonNull Set<String> listenQueues) {

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

  public static @NonNull DBOSConfig defaults(@NonNull String appName) {
    return new DBOSConfig(
        appName, null, null, null, null, false, // adminServer
        3001, // adminServerPort
        true, // migrate
        null, null, null, null, null, false, null);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        v,
        executorId,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        v,
        databaseSchema,
        enablePatching,
        listenQueues);
  }

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
        appVersion,
        executorId,
        v,
        enablePatching,
        listenQueues);
  }

  public @NonNull DBOSConfig withEnablePatching() {
    return this.withEnablePatching(true);
  }

  public @NonNull DBOSConfig withDisablePatching() {
    return this.withEnablePatching(false);
  }

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
        appVersion,
        executorId,
        databaseSchema,
        v,
        listenQueues);
  }

  public @NonNull DBOSConfig enableAdminServer() {
    return withAdminServer(true);
  }

  public @NonNull DBOSConfig disableAdminServer() {
    return withAdminServer(false);
  }

  public @NonNull DBOSConfig withListenQueue(@NonNull Queue queue) {
    return withListenQueues(new String[] {queue.name()});
  }

  public @NonNull DBOSConfig withListenQueue(@NonNull String queueName) {
    return withListenQueues(new String[] {queueName});
  }

  public @NonNull DBOSConfig withListenQueues(@Nullable Queue... queues) {
    var names =
        Arrays.stream(Objects.requireNonNullElse(queues, new Queue[0])).map(Queue::name).toList();
    return withListenQueues(names.toArray(String[]::new));
  }

  public @NonNull DBOSConfig withListenQueues(@Nullable String... queueNames) {
    var v =
        Stream.concat(
                listenQueues.stream(),
                Arrays.stream(Objects.requireNonNullElse(queueNames, new String[0])))
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
        appVersion,
        executorId,
        databaseSchema,
        enablePatching,
        v);
  }

  // Override toString to mask the DB password
  @Override
  public String toString() {
    return ("DBOSConfig[appName=%s, databaseUrl=%s, dbUser=%s, dbPassword=***, "
            + "dataSource=%s, dbSchema=%s, adminServer=%s, adminServerPort=%d, "
            + "migrate=%s, conductorKey=%s, conductorDomain=%s, "
            + "appVersion=%s, executorId=%s, enablePatching=%s, listenQueues=%s]")
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
            appVersion,
            executorId,
            enablePatching,
            listenQueues);
  }
}
