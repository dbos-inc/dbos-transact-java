package dev.dbos.transact.spring;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration properties for DBOS Transact, bound to the {@code dbos.*} namespace. */
@ConfigurationProperties(prefix = "dbos")
public class DBOSProperties {

  /** Application identity: name (required) and version. */
  private Application application = new Application();

  /**
   * Dedicated datasource for the DBOS system database. When not set, DBOS will use the
   * application's primary {@code DataSource} bean if one is available in the Spring context.
   */
  private Datasource datasource = new Datasource();

  /** DBOS Cloud conductor connection settings. */
  private Conductor conductor = new Conductor();

  /** Admin HTTP server settings. */
  private AdminServer adminServer = new AdminServer();

  /** Executor ID for this instance. */
  private String executorId;

  /** Whether to enable workflow patching. Defaults to {@code false}. */
  private boolean enablePatching = false;

  /** Names of queues this executor should listen on. */
  private List<String> listenQueues = new ArrayList<>();

  /** Polling interval for the workflow scheduler. */
  private Duration schedulerPollingInterval;

  /** Admin HTTP server properties. */
  public static class AdminServer {

    /** Whether to enable the admin HTTP server. Defaults to {@code false}. */
    private boolean enabled;

    /** Port for the admin HTTP server. Defaults to {@code 3001}. */
    private int port = 3001;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }
  }

  /** Application identity properties. */
  public static class Application {

    /** Application name. Required if {@code spring.application.name} is not set. */
    private String name;

    /** Application version string. */
    private String version;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getVersion() {
      return version;
    }

    public void setVersion(String version) {
      this.version = version;
    }
  }

  /** DBOS Cloud conductor connection properties. */
  public static class Conductor {

    /** Conductor API key. */
    private String key;

    /** Conductor domain. */
    private String domain;

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public String getDomain() {
      return domain;
    }

    public void setDomain(String domain) {
      this.domain = domain;
    }
  }

  /** Dedicated datasource configuration for the DBOS system database. */
  public static class Datasource {

    /** JDBC URL for the DBOS system database. */
    private String url;

    /** Database username. */
    private String username;

    /** Database password. */
    private String password;

    /** Database schema name for DBOS system tables. */
    private String schema;

    /** Whether to run database migrations on startup. Defaults to {@code true}. */
    private boolean migrate = true;

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

    public String getSchema() {
      return schema;
    }

    public void setSchema(String schema) {
      this.schema = schema;
    }

    public boolean isMigrate() {
      return migrate;
    }

    public void setMigrate(boolean migrate) {
      this.migrate = migrate;
    }
  }

  public Application getApplication() {
    return application;
  }

  public void setApplication(Application application) {
    this.application = application;
  }

  public Datasource getDatasource() {
    return datasource;
  }

  public void setDatasource(Datasource datasource) {
    this.datasource = datasource;
  }

  public AdminServer getAdminServer() {
    return adminServer;
  }

  public void setAdminServer(AdminServer adminServer) {
    this.adminServer = adminServer;
  }

  public Conductor getConductor() {
    return conductor;
  }

  public void setConductor(Conductor conductor) {
    this.conductor = conductor;
  }

  public String getExecutorId() {
    return executorId;
  }

  public void setExecutorId(String executorId) {
    this.executorId = executorId;
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
