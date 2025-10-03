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

  static Logger logger = LoggerFactory.getLogger(DBOSConfig.class);

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

    public DBOSConfig build() {
      if (appName == null) throw new IllegalArgumentException("Name is required");

      if (dbPassword == null) {
        dbPassword = System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR);
      }
      if (databaseUrl == null) {
        databaseUrl = System.getenv(Constants.JDBC_URL_ENV_VAR);
        logger.info("Using db_url env {}", databaseUrl);
      }
      if (dbUser == null) {
        dbUser = System.getenv(Constants.POSTGRES_USER_ENV_VAR);
      }

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
}
