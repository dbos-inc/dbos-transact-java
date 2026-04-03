package dev.dbos.transact.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class DBOSAutoConfigurationTest {

  // Base runner: provides a mock DBOS so DBOSLifecycle.start() is a no-op (no real DB needed).
  // The mock satisfies @ConditionalOnMissingBean(DBOS.class), so auto-config skips DBOS creation
  // but still creates DBOSConfig, DBOSLifecycle, DBOSAspect, and DBOSWorkflowRegistrar.
  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(DBOSAutoConfiguration.class))
          .withPropertyValues("dbos.app-name=test-app")
          .withBean(DBOS.class, () -> mock(DBOS.class));

  @Test
  void createsExpectedBeans() {
    runner.run(
        context -> {
          assertThat(context).hasSingleBean(DBOSConfig.class);
          assertThat(context).hasSingleBean(DBOS.class);
          assertThat(context).hasSingleBean(DBOSAutoConfiguration.DBOSLifecycle.class);
          assertThat(context).hasSingleBean(DBOSAspect.class);
          assertThat(context).hasSingleBean(DBOSWorkflowRegistrar.class);
        });
  }

  @Test
  void dbosConfigReflectsAppNameProperty() {
    runner.run(
        context -> {
          var config = context.getBean(DBOSConfig.class);
          assertThat(config.appName()).isEqualTo("test-app");
        });
  }

  @Test
  void dbosConfigReflectsDatasourceProperties() {
    runner
        .withPropertyValues(
            "dbos.datasource.url=jdbc:postgresql://localhost/db",
            "dbos.datasource.username=user",
            "dbos.datasource.password=pass")
        .run(
            context -> {
              var config = context.getBean(DBOSConfig.class);
              assertThat(config.databaseUrl()).isEqualTo("jdbc:postgresql://localhost/db");
              assertThat(config.dbUser()).isEqualTo("user");
              assertThat(config.dbPassword()).isEqualTo("pass");
            });
  }

  @Test
  void dbosConfigReflectsOptionalProperties() {
    runner
        .withPropertyValues(
            "dbos.conductor-key=my-key",
            "dbos.conductor-domain=my-domain",
            "dbos.app-version=1.2.3",
            "dbos.executor-id=exec-1",
            "dbos.database-schema=my_schema",
            "dbos.admin-server=true",
            "dbos.admin-server-port=9090",
            "dbos.migrate=false",
            "dbos.enable-patching=true",
            "dbos.listen-queues[0]=q1",
            "dbos.listen-queues[1]=q2")
        .run(
            context -> {
              var config = context.getBean(DBOSConfig.class);
              assertThat(config.conductorKey()).isEqualTo("my-key");
              assertThat(config.conductorDomain()).isEqualTo("my-domain");
              assertThat(config.appVersion()).isEqualTo("1.2.3");
              assertThat(config.executorId()).isEqualTo("exec-1");
              assertThat(config.databaseSchema()).isEqualTo("my_schema");
              assertThat(config.adminServer()).isTrue();
              assertThat(config.adminServerPort()).isEqualTo(9090);
              assertThat(config.migrate()).isFalse();
              assertThat(config.enablePatching()).isTrue();
              assertThat(config.listenQueues()).containsExactlyInAnyOrder("q1", "q2");
            });
  }

  @Test
  void userProvidedDbosConfigPreventsBeanCreation() {
    var customConfig = DBOSConfig.defaults("custom-app");
    runner
        .withBean(DBOSConfig.class, () -> customConfig)
        .run(
            context -> {
              assertThat(context).hasSingleBean(DBOSConfig.class);
              assertThat(context.getBean(DBOSConfig.class)).isSameAs(customConfig);
            });
  }

  @Test
  void userProvidedDbosPreventsBeanCreation() {
    var customDbos = mock(DBOS.class);
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(DBOSAutoConfiguration.class))
        .withPropertyValues("dbos.app-name=test-app")
        .withBean("customDbos", DBOS.class, () -> customDbos)
        .run(
            context -> {
              assertThat(context).hasSingleBean(DBOS.class);
              assertThat(context.getBean(DBOS.class)).isSameAs(customDbos);
            });
  }

  @Test
  void configCustomizerIsApplied() {
    runner
        .withBean(DBOSConfigCustomizer.class, () -> config -> config.withExecutorId("custom-exec"))
        .run(
            context -> {
              var config = context.getBean(DBOSConfig.class);
              assertThat(config.executorId()).isEqualTo("custom-exec");
            });
  }

  @Test
  void multipleCustomizersAreApplied() {
    runner
        .withBean(
            "customizer1",
            DBOSConfigCustomizer.class,
            () -> config -> config.withExecutorId("exec-1"))
        .withBean(
            "customizer2", DBOSConfigCustomizer.class, () -> config -> config.withAppVersion("2.0"))
        .run(
            context -> {
              var config = context.getBean(DBOSConfig.class);
              assertThat(config.executorId()).isEqualTo("exec-1");
              assertThat(config.appVersion()).isEqualTo("2.0");
            });
  }

  @Test
  void dataSourceBeanIsUsedWhenNoDatasourceUrlConfigured() {
    // When a DataSource bean is present and no dbos.datasource.url is set,
    // the DBOS instance should be created using that DataSource (no databaseUrl required).
    // We provide a mock DBOSLifecycle to prevent dbos.launch() from running.
    var mockDs = mock(DataSource.class);
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(DBOSAutoConfiguration.class))
        .withPropertyValues("dbos.app-name=test-app")
        .withBean(DataSource.class, () -> mockDs)
        .withBean(
            DBOSAutoConfiguration.DBOSLifecycle.class,
            () -> mock(DBOSAutoConfiguration.DBOSLifecycle.class))
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context).hasSingleBean(DBOS.class);
            });
  }
}
