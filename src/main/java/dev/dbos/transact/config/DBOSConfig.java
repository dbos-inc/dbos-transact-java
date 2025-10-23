package dev.dbos.transact.config;

import dev.dbos.transact.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record DBOSConfig(
    String appName,
    String databaseUrl,
    String dbUser,
    String dbPassword,
    int maximumPoolSize,
    int connectionTimeout,
    boolean adminServer,
    int adminServerPort,
    boolean migrate,
    String conductorKey,
    String conductorDomain,
    String appVersion,
    String executorId) {

  private static final Logger logger = LoggerFactory.getLogger(DBOSConfig.class);

  public static DBOSConfig defaults(String appName) {
    return new DBOSConfig(
        appName, null, null, null, 3, // maximumPoolSize default
        30000, // connectionTimeout default
        false, // adminServer
        3001, // adminServerPort
        true, // migrate
        null, null, null, null);
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
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId);
  }

  public DBOSConfig withDatabaseUrl(String v) {
    return new DBOSConfig(
        appName,
        v,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId);
  }

  public DBOSConfig withDbUser(String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        v,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId);
  }

  public DBOSConfig withDbPassword(String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        v,
        maximumPoolSize,
        connectionTimeout,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId);
  }

  public DBOSConfig withMaximumPoolSize(int v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        v,
        connectionTimeout,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId);
  }

  public DBOSConfig withConnectionTimeout(int v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        v,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId);
  }

  public DBOSConfig withAdminServer(boolean v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        v,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId);
  }

  public DBOSConfig withAdminServerPort(int v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        adminServer,
        v,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId);
  }

  public DBOSConfig withMigrate(boolean v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        adminServer,
        adminServerPort,
        v,
        conductorKey,
        conductorDomain,
        appVersion,
        executorId);
  }

  public DBOSConfig withConductorKey(String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        adminServer,
        adminServerPort,
        migrate,
        v,
        conductorDomain,
        appVersion,
        executorId);
  }

  public DBOSConfig withConductorDomain(String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        v,
        appVersion,
        executorId);
  }

  public DBOSConfig withAppVersion(String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        v,
        executorId);
  }

  public DBOSConfig withExecutorId(String v) {
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        maximumPoolSize,
        connectionTimeout,
        adminServer,
        adminServerPort,
        migrate,
        conductorKey,
        conductorDomain,
        appVersion,
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
    return "DBOSConfig[appName=%s, databaseUrl=%s, dbUser=%s, dbPassword=***, maximumPoolSize=%d, connectionTimeout=%d, adminServer=%s, adminServerPort=%d, migrate=%s, conductorKey=%s, conductorDomain=%s, appVersion=%s, executorId=%s]"
        .formatted(
            appName,
            databaseUrl,
            dbUser,
            maximumPoolSize,
            connectionTimeout,
            adminServer,
            adminServerPort,
            migrate,
            conductorKey,
            conductorDomain,
            appVersion,
            executorId);
  }
}
