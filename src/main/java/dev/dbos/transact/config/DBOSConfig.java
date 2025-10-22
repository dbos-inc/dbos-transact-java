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

  public static DBOSConfig defaultsFromEnv(String appName) {
    String databaseUrl = System.getenv(Constants.SYSTEM_JDBC_URL_ENV_VAR);
    String dbUser = System.getenv(Constants.POSTGRES_USER_ENV_VAR);
    String dbPassword = System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR);
    return new DBOSConfig(
        appName,
        databaseUrl,
        dbUser,
        dbPassword,
        3, // maximumPoolSize default
        30000, // connectionTimeout default
        false, // adminServer
        3001, // adminServerPort
        true, // migrate
        null,
        null,
        null,
        null);
  }

  public static class Builder {
    private String appName;
    private String databaseUrl;
    private String dbUser;
    private String dbPassword;
    private int maximumPoolSize = 3;
    private int connectionTimeout = 30000;
    private boolean adminServer = false;
    private int adminServerPort = 3001;
    private boolean migrate = true;
    private String conductorKey;
    private String conductorDomain;
    private String appVersion;
    private String executorId;

    public Builder appName(String appName) {
      this.appName = appName;
      return this;
    }

    public Builder databaseUrl(String databaseUrl) {
      this.databaseUrl = databaseUrl;
      return this;
    }

    public Builder dbUser(String dbUser) {
      this.dbUser = dbUser;
      return this;
    }

    public Builder dbPassword(String dbPassword) {
      this.dbPassword = dbPassword;
      return this;
    }

    public Builder maximumPoolSize(int maximumPoolSize) {
      this.maximumPoolSize = maximumPoolSize;
      return this;
    }

    public Builder connectionTimeout(int connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
    }

    public Builder runAdminServer() {
      this.adminServer = true;
      return this;
    }

    public Builder runAdminServer(boolean adminServer) {
      this.adminServer = adminServer;
      return this;
    }

    public Builder adminServerPort(int port) {
      this.adminServerPort = port;
      return this;
    }

    public Builder migration(boolean migrate) {
      this.migrate = migrate;
      return this;
    }

    public Builder conductorKey(String key) {
      this.conductorKey = key;
      return this;
    }

    public Builder conductorDomain(String domain) {
      this.conductorDomain = domain;
      return this;
    }

    public Builder appVersion(String appVersion) {
      this.appVersion = appVersion;
      return this;
    }

    public Builder executorId(String executorId) {
      this.executorId = executorId;
      return this;
    }

    /** Load missing JDBC/PG creds from env if absent. */
    public Builder defaultsFromEnv() {
      if (databaseUrl == null) databaseUrl = System.getenv(Constants.SYSTEM_JDBC_URL_ENV_VAR);
      if (dbUser == null) dbUser = System.getenv(Constants.POSTGRES_USER_ENV_VAR);
      if (dbPassword == null) dbPassword = System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR);
      return this;
    }

    /** Prefer: pure build() (no env reads, no logs). */
    public DBOSConfig build() {
      if (appName == null) throw new IllegalArgumentException("Name is required");
      defaultsFromEnv();
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
          executorId);
    }
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
