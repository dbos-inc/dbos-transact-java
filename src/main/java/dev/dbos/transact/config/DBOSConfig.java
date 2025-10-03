package dev.dbos.transact.config;

import dev.dbos.transact.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record DBOSConfig(
    String url,
    String name,
    String dbHost,
    int dbPort,
    String dbUser,
    String dbPassword,
    int maximumPoolSize,
    int connectionTimeout,
    String sysDbName,
    boolean adminServer,
    int adminServerPort,
    boolean migrate,
    String conductorKey,
    String conductorDomain) {

  static Logger logger = LoggerFactory.getLogger(DBOSConfig.class);

  public static class Builder {
    private String name;
    private String url;
    private String dbHost;
    private int dbPort;
    private String dbUser;
    private String dbPassword;
    private int maximumPoolSize = 3;
    private int connectionTimeout = 30000;
    private String sysDbName;
    private boolean adminServer = false;
    private int adminServerPort = 3001;
    private boolean migrate = true;
    private String conductorKey;
    private String conductorDomain;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder url(String url) {
      this.url = url;
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

    public Builder dbHost(String dbHost) {
      this.dbHost = dbHost;
      return this;
    }

    public Builder dbPort(int dbPort) {
      this.dbPort = dbPort;
      return this;
    }

    public Builder sysDbName(String sysDbName) {
      this.sysDbName = sysDbName;
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

    public DBOSConfig build() {
      if (name == null) throw new IllegalArgumentException("Name is required");

      if (dbPassword == null) {
        dbPassword = System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR);
      }
      if (url == null) {
        url = System.getenv(Constants.JDBC_URL_ENV_VAR);
        logger.info("Using db_url env {}", url);
      }
      if (dbUser == null) {
        dbUser = System.getenv(Constants.POSTGRES_USER_ENV_VAR);
      }

      if (sysDbName == null) {
        sysDbName = name + Constants.SYS_DB_SUFFIX;
      }

      return new DBOSConfig(
          url,
          name,
          dbHost,
          dbPort,
          dbUser,
          dbPassword,
          maximumPoolSize,
          connectionTimeout,
          sysDbName,
          adminServer,
          adminServerPort,
          migrate,
          conductorKey,
          conductorDomain);
    }
  }
}
