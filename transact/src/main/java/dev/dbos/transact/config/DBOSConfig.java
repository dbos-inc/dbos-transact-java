package dev.dbos.transact.config;

import dev.dbos.transact.Constants;

import com.zaxxer.hikari.HikariDataSource;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public record DBOSConfig(
    @NonNull String appName,
    @Nullable String databaseUrl,
    @Nullable String dbUser,
    @Nullable String dbPassword,
    int maximumPoolSize,
    int connectionTimeout,
    @Nullable HikariDataSource dataSource,
    boolean adminServer,
    int adminServerPort,
    boolean migrate,
    @Nullable String conductorKey,
    @Nullable String conductorDomain,
    @Nullable String appVersion,
    @Nullable String executorId,
    @Nullable String databaseSchema,
    boolean enablePatching) {

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
  }

  public static @NonNull DBOSConfig defaults(@NonNull String appName) {
    return new DBOSConfig(
        appName, null, null, null, 3, // maximumPoolSize default
        30000, // connectionTimeout default
        null, false, // adminServer
        3001, // adminServerPort
        true, // migrate
        null, null, null, null, null, false);
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
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withDatabaseUrl(@Nullable String v) {
    return new DBOSConfig(
        appName,
        v,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withDbUser(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        v,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withDbPassword(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        v,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withMaximumPoolSize(int v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        v,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withConnectionTimeout(int v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
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
        enablePatching);
  }

  public @NonNull DBOSConfig withDataSource(@Nullable HikariDataSource v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        v,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withAdminServer(boolean v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        v,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withAdminServerPort(int v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        v,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withMigrate(boolean v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        v,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withConductorKey(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        v,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withConductorDomain(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        v,
        appVersion,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withAppVersion(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        v,
        executorId,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withExecutorId(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        v,
        databaseSchema,
        enablePatching);
  }

  public @NonNull DBOSConfig withDatabaseSchema(@Nullable String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        v,
        enablePatching);
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
        maximumPoolSize,
        connectionTimeout,
        dataSource,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId,
        databaseSchema,
        v);
  }

  public @NonNull DBOSConfig enableAdminServer() {
    return withAdminServer(true);
  }

  public @NonNull DBOSConfig disableAdminServer() {
    return withAdminServer(false);
  }

  // Override toString to mask the DB password
  @Override
  public String toString() {
    return "DBOSConfig[appName=%s, databaseUrl=%s, dbUser=%s, dbPassword=***, maximumPoolSize=%d, connectionTimeout=%d, dataSource=%s, adminServer=%s, adminServerPort=%d, migrate=%s, conductorKey=%s, conductorDomain=%s, appVersion=%s, executorId=%s, dbSchema=%s, enablePatching=%s]"
        .formatted(
            appName,
            databaseUrl,
            dbUser,
            maximumPoolSize,
            connectionTimeout,
            dataSource,
            adminServer,
            adminServerPort,
            migrate,
            conductorKey,
            conductorDomain,
            appVersion,
            executorId,
            databaseSchema,
            enablePatching);
  }
}
