package dev.dbos.transact.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class DBOSPropertiesTest {

  private static DBOSProperties bind(Map<String, String> props) {
    var source = new MapConfigurationPropertySource(props);
    return new Binder(source).bind("dbos", DBOSProperties.class).get();
  }

  @Test
  void defaults() {
    var props = new DBOSProperties();
    assertNull(props.getAppName());
    assertFalse(props.isAdminServer());
    assertEquals(3001, props.getAdminServerPort());
    assertTrue(props.isMigrate());
    assertFalse(props.isEnablePatching());
    assertTrue(props.getListenQueues().isEmpty());
    assertNull(props.getConductorKey());
    assertNull(props.getConductorDomain());
    assertNull(props.getAppVersion());
    assertNull(props.getExecutorId());
    assertNull(props.getDatabaseSchema());
    assertNull(props.getSchedulerPollingInterval());
    assertNotNull(props.getDatasource());
    assertNull(props.getDatasource().getUrl());
    assertNull(props.getDatasource().getUsername());
    assertNull(props.getDatasource().getPassword());
  }

  @Test
  void bindsAppName() {
    var props = bind(Map.of("dbos.app-name", "my-app"));
    assertEquals("my-app", props.getAppName());
  }

  @Test
  void bindsDatasource() {
    var props =
        bind(
            Map.of(
                "dbos.datasource.url", "jdbc:postgresql://localhost/db",
                "dbos.datasource.username", "user",
                "dbos.datasource.password", "pass"));
    assertEquals("jdbc:postgresql://localhost/db", props.getDatasource().getUrl());
    assertEquals("user", props.getDatasource().getUsername());
    assertEquals("pass", props.getDatasource().getPassword());
  }

  @Test
  void bindsAdminServerProperties() {
    var props = bind(Map.of("dbos.admin-server", "true", "dbos.admin-server-port", "9090"));
    assertTrue(props.isAdminServer());
    assertEquals(9090, props.getAdminServerPort());
  }

  @Test
  void bindsMigrateAndPatching() {
    var props = bind(Map.of("dbos.migrate", "false", "dbos.enable-patching", "true"));
    assertFalse(props.isMigrate());
    assertTrue(props.isEnablePatching());
  }

  @Test
  void bindsConductorProperties() {
    var props = bind(Map.of("dbos.conductor-key", "my-key", "dbos.conductor-domain", "my-domain"));
    assertEquals("my-key", props.getConductorKey());
    assertEquals("my-domain", props.getConductorDomain());
  }

  @Test
  void bindsIdentityProperties() {
    var props =
        bind(
            Map.of(
                "dbos.app-version", "1.2.3",
                "dbos.executor-id", "exec-1",
                "dbos.database-schema", "my_schema"));
    assertEquals("1.2.3", props.getAppVersion());
    assertEquals("exec-1", props.getExecutorId());
    assertEquals("my_schema", props.getDatabaseSchema());
  }

  @Test
  void bindsSchedulerPollingInterval() {
    var props = bind(Map.of("dbos.scheduler-polling-interval", "30s"));
    assertEquals(Duration.ofSeconds(30), props.getSchedulerPollingInterval());
  }

  @Test
  void bindsListenQueues() {
    var props =
        bind(
            Map.of(
                "dbos.listen-queues[0]", "queue-a",
                "dbos.listen-queues[1]", "queue-b"));
    assertEquals(List.of("queue-a", "queue-b"), props.getListenQueues());
  }
}
