package dev.dbos.transact.config;

import dev.dbos.transact.Constants;

import com.zaxxer.hikari.HikariDataSource;

public record DBOSConfig(
    String appName,
    String databaseUrl,
    String dbUser,
    String dbPassword,
    int maximumPoolSize,
    int connectionTimeout,
    HikariDataSource dataSource,
    boolean adminServer,
    int adminServerPort,
    boolean migrate,
    String conductorKey,
    String conductorDomain,
    String appVersion,
    String executorId,
    String databaseSchema,
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

  public static DBOSConfig defaults(String appName) {
    return new DBOSConfig(
        appName, null, null, null, 3, // maximumPoolSize default
        30000, // connectionTimeout default
        null, false, // adminServer
        3001, // adminServerPort
        true, // migrate
        null, null, null, null, null, false);
  }

  public static DBOSConfig defaultsFromEnv(String appName) {
    String databaseUrl = System.getenv(Constants.SYSTEM_JDBC_URL_ENV_VAR);
    String dbUser = System.getenv(Constants.POSTGRES_USER_ENV_VAR);
    if (dbUser == null || dbUser.isEmpty()) dbUser = "postgres";
    String dbPassword = System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR);
    return defaults(appName)
        .withDatabaseUrl(databaseUrl)
        .withDbUser(dbUser)
        .withDbPassword(dbPassword);
  }

  public DBOSConfig withAppName(String v) {
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

  public DBOSConfig withDatabaseUrl(String v) {
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

  public DBOSConfig withDbUser(String v) {
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

  public DBOSConfig withDbPassword(String v) {
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

  public DBOSConfig withMaximumPoolSize(int v) {
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

  public DBOSConfig withConnectionTimeout(int v) {
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

  public DBOSConfig withDataSource(HikariDataSource v) {
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

  public DBOSConfig withAdminServer(boolean v) {
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

  public DBOSConfig withAdminServerPort(int v) {
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

  public DBOSConfig withMigrate(boolean v) {
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

  public DBOSConfig withConductorKey(String v) {
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

  public DBOSConfig withConductorDomain(String v) {
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

  public DBOSConfig withAppVersion(String v) {
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

  public DBOSConfig withExecutorId(String v) {
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

  public DBOSConfig withDatabaseSchema(String v) {
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

  public DBOSConfig withEnablePatching() {
    return this.withEnablePatching(true);
  }

  public DBOSConfig withDisablePatching() {
    return this.withEnablePatching(false);
  }

  public DBOSConfig withEnablePatching(boolean v) {
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

  public DBOSConfig enableAdminServer() {
    return withAdminServer(true);
  }

  public DBOSConfig disableAdminServer() {
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
