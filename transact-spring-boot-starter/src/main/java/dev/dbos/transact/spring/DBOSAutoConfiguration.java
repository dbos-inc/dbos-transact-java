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
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * Spring Boot auto-configuration for DBOS Transact. Creates a {@link DBOSConfig} and {@link DBOS}
 * bean from {@code dbos.*} application properties and manages the DBOS lifecycle alongside the
 * Spring application context.
 *
 * <p>The {@link DBOS} instance is started (via {@link DBOS#launch()}) after all other beans have
 * been initialized, so workflows and queues may be registered in {@code @PostConstruct} methods
 * before launch occurs.
 *
 * <p>To customize the auto-configured {@link DBOSConfig} without replacing it, declare one or more
 * {@link DBOSConfigCustomizer} beans. To replace it entirely, declare your own {@code @Bean
 * DBOSConfig}.
 *
 * <p>If a {@link DataSource} bean is present in the context and no {@code dbos.datasource.*}
 * properties are set, that datasource will be used automatically.
 */
@AutoConfiguration
@ConditionalOnClass(DBOS.class)
@EnableConfigurationProperties(DBOSProperties.class)
@EnableAspectJAutoProxy
public class DBOSAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public DBOSConfig dbosConfig(
      DBOSProperties props, ObjectProvider<DBOSConfigCustomizer> customizers) {
    DBOSConfig config = buildConfig(props);
    for (DBOSConfigCustomizer customizer : customizers.orderedStream().toList()) {
      config = customizer.customize(config);
    }
    return config;
  }

  @Bean
  @ConditionalOnMissingBean
  public DBOS dbos(DBOSConfig config, ObjectProvider<DataSource> dataSourceProvider) {
    if (config.databaseUrl() == null && config.dataSource() == null) {
      DataSource dataSource = dataSourceProvider.getIfAvailable();
      if (dataSource != null) {
        config = config.withDataSource(dataSource);
      }
    }
    return new DBOS(config);
  }

  @Bean
  @ConditionalOnMissingBean
  public DBOSLifecycle dbosLifecycle(DBOS dbos) {
    return new DBOSLifecycle(dbos);
  }

  @Bean
  @ConditionalOnMissingBean
  public DBOSAspect dbosAspect(DBOS dbos, ApplicationContext applicationContext) {
    return new DBOSAspect(dbos, applicationContext);
  }

  @Bean
  @ConditionalOnMissingBean
  public DBOSWorkflowRegistrar dbosWorkflowRegistrar(
      DBOS dbos, ApplicationContext applicationContext) {
    return new DBOSWorkflowRegistrar(dbos, applicationContext);
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
