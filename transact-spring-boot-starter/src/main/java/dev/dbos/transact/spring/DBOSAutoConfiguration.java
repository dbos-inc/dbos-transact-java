package dev.dbos.transact.spring;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration for DBOS Transact. Creates a {@link DBOS} bean from {@code dbos.*}
 * application properties and manages its lifecycle alongside the Spring application context.
 *
 * <p>The {@link DBOS} instance is started (via {@link DBOS#launch()}) after all other beans have
 * been initialized, so workflows and queues may be registered in {@code @PostConstruct} methods
 * before launch occurs.
 *
 * <p>If a {@link DataSource} bean is present in the context it will be used automatically, unless
 * {@code dbos.database-url} is also set in which case the explicit URL takes precedence.
 */
@AutoConfiguration
@ConditionalOnClass(DBOS.class)
@EnableConfigurationProperties(DBOSProperties.class)
public class DBOSAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public DBOS dbos(DBOSProperties props, ObjectProvider<DataSource> dataSourceProvider) {
    DBOSConfig config = buildConfig(props);
    DataSource dataSource = dataSourceProvider.getIfAvailable();
    if (dataSource != null && config.databaseUrl() == null && config.dataSource() == null) {
      config = config.withDataSource(dataSource);
    }
    return new DBOS(config);
  }

  @Bean
  @ConditionalOnMissingBean
  public DBOSLifecycle dbosLifecycle(DBOS dbos) {
    return new DBOSLifecycle(dbos);
  }

  private DBOSConfig buildConfig(DBOSProperties props) {
    DBOSConfig config = DBOSConfig.defaults(props.getAppName());

    DBOSProperties.Datasource ds = props.getDatasource();
    if (ds.getUrl() != null) {
      config = config.withDatabaseUrl(ds.getUrl());
    }
    if (ds.getUsername() != null) {
      config = config.withDbUser(ds.getUsername());
    }
    if (ds.getPassword() != null) {
      config = config.withDbPassword(ds.getPassword());
    }
    if (props.getConductorKey() != null) {
      config = config.withConductorKey(props.getConductorKey());
    }
    if (props.getConductorDomain() != null) {
      config = config.withConductorDomain(props.getConductorDomain());
    }
    if (props.getAppVersion() != null) {
      config = config.withAppVersion(props.getAppVersion());
    }
    if (props.getExecutorId() != null) {
      config = config.withExecutorId(props.getExecutorId());
    }
    if (props.getDatabaseSchema() != null) {
      config = config.withDatabaseSchema(props.getDatabaseSchema());
    }
    if (props.getSchedulerPollingInterval() != null) {
      config = config.withSchedulerPollingInterval(props.getSchedulerPollingInterval());
    }

    config = config.withAdminServer(props.isAdminServer());
    config = config.withAdminServerPort(props.getAdminServerPort());
    config = config.withMigrate(props.isMigrate());
    config = config.withEnablePatching(props.isEnablePatching());

    List<String> listenQueues = props.getListenQueues();
    if (!listenQueues.isEmpty()) {
      config = config.withListenQueues(listenQueues.toArray(String[]::new));
    }

    return config;
  }

  /**
   * Manages the {@link DBOS} lifecycle within the Spring application context. {@link DBOS#launch()}
   * is called after all beans are initialized; {@link DBOS#shutdown()} is called on context close.
   */
  public static class DBOSLifecycle implements SmartLifecycle {

    private final DBOS dbos;
    private volatile boolean running = false;

    public DBOSLifecycle(DBOS dbos) {
      this.dbos = dbos;
    }

    @Override
    public void start() {
      dbos.launch();
      running = true;
    }

    @Override
    public void stop() {
      dbos.shutdown();
      running = false;
    }

    @Override
    public boolean isRunning() {
      return running;
    }

    /** High phase value ensures DBOS starts last (after all beans are ready) and stops first. */
    @Override
    public int getPhase() {
      return DEFAULT_PHASE;
    }
  }
}
