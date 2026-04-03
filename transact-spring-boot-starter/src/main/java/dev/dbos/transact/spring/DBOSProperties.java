package dev.dbos.transact.spring;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration properties for DBOS Transact, bound to the {@code dbos.*} namespace. */
@ConfigurationProperties(prefix = "dbos")
public class DBOSProperties {

  /** Application name. Required. */
  private String appName;

  /**
   * Dedicated datasource for the DBOS system database. When not set, DBOS will use the
   * application's primary {@code DataSource} bean if one is available in the Spring context.
   */
  private Datasource datasource = new Datasource();

  /** Whether to enable the DBOS admin HTTP server. Defaults to {@code false}. */
  private boolean adminServer = false;

  /** Port for the DBOS admin HTTP server. Defaults to {@code 3001}. */
  private int adminServerPort = 3001;

  /** Whether to run database migrations on startup. Defaults to {@code true}. */
  private boolean migrate = true;

  /** DBOS Cloud conductor API key. */
  private String conductorKey;

  /** DBOS Cloud conductor domain. */
  private String conductorDomain;

  /** Application version string. */
  private String appVersion;

  /** Executor ID for this instance. */
  private String executorId;

  /** Database schema name for DBOS system tables. */
  private String databaseSchema;

  /** Whether to enable workflow patching. Defaults to {@code false}. */
  private boolean enablePatching = false;

  /** Names of queues this executor should listen on. */
  private List<String> listenQueues = new ArrayList<>();

  /** Polling interval for the workflow scheduler. */
  private Duration schedulerPollingInterval;

  /** Dedicated datasource configuration for the DBOS system database. */
  public static class Datasource {

    /** JDBC URL for the DBOS system database. */
    private String url;

    /** Database username. */
    private String username;

    /** Database password. */
    private String password;

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public Datasource getDatasource() {
    return datasource;
  }

  public void setDatasource(Datasource datasource) {
    this.datasource = datasource;
  }

  public boolean isAdminServer() {
    return adminServer;
  }

  public void setAdminServer(boolean adminServer) {
    this.adminServer = adminServer;
  }

  public int getAdminServerPort() {
    return adminServerPort;
  }

  public void setAdminServerPort(int adminServerPort) {
    this.adminServerPort = adminServerPort;
  }

  public boolean isMigrate() {
    return migrate;
  }

  public void setMigrate(boolean migrate) {
    this.migrate = migrate;
  }

  public String getConductorKey() {
    return conductorKey;
  }

  public void setConductorKey(String conductorKey) {
    this.conductorKey = conductorKey;
  }

  public String getConductorDomain() {
    return conductorDomain;
  }

  public void setConductorDomain(String conductorDomain) {
    this.conductorDomain = conductorDomain;
  }

  public String getAppVersion() {
    return appVersion;
  }

  public void setAppVersion(String appVersion) {
    this.appVersion = appVersion;
  }

  public String getExecutorId() {
    return executorId;
  }

  public void setExecutorId(String executorId) {
    this.executorId = executorId;
  }

  public String getDatabaseSchema() {
    return databaseSchema;
  }

  public void setDatabaseSchema(String databaseSchema) {
    this.databaseSchema = databaseSchema;
  }

  public boolean isEnablePatching() {
    return enablePatching;
  }

  public void setEnablePatching(boolean enablePatching) {
    this.enablePatching = enablePatching;
  }

  public List<String> getListenQueues() {
    return listenQueues;
  }

  public void setListenQueues(List<String> listenQueues) {
    this.listenQueues = listenQueues;
  }

  public Duration getSchedulerPollingInterval() {
    return schedulerPollingInterval;
  }

  public void setSchedulerPollingInterval(Duration schedulerPollingInterval) {
    this.schedulerPollingInterval = schedulerPollingInterval;
  }
}
