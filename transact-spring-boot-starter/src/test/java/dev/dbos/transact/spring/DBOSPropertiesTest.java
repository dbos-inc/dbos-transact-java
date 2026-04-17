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
    assertNotNull(props.getApplication());
    assertNull(props.getApplication().getName());
    assertNull(props.getApplication().getVersion());
    assertNotNull(props.getAdminServer());
    assertFalse(props.getAdminServer().isEnabled());
    assertEquals(3001, props.getAdminServer().getPort());
    assertTrue(props.getDatasource().isMigrate());
    assertFalse(props.isEnablePatching());
    assertTrue(props.getListenQueues().isEmpty());
    assertNotNull(props.getConductor());
    assertNull(props.getConductor().getKey());
    assertNull(props.getConductor().getDomain());
    assertNull(props.getExecutorId());
    assertNull(props.getDatasource().getSchema());
    assertNull(props.getSchedulerPollingInterval());
    assertNotNull(props.getDatasource());
    assertNull(props.getDatasource().getUrl());
    assertNull(props.getDatasource().getUsername());
    assertNull(props.getDatasource().getPassword());
    assertNull(props.getDatasource().getSchema());
    assertTrue(props.getDatasource().isMigrate());
  }

  @Test
  void bindsApplicationName() {
    var props = bind(Map.of("dbos.application.name", "my-app"));
    assertEquals("my-app", props.getApplication().getName());
  }

  @Test
  void bindsApplicationVersion() {
    var props = bind(Map.of("dbos.application.version", "1.2.3"));
    assertEquals("1.2.3", props.getApplication().getVersion());
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
    var props = bind(Map.of("dbos.admin-server.enabled", "true", "dbos.admin-server.port", "9090"));
    assertTrue(props.getAdminServer().isEnabled());
    assertEquals(9090, props.getAdminServer().getPort());
  }

  @Test
  void bindsMigrateAndPatching() {
    var props = bind(Map.of("dbos.datasource.migrate", "false", "dbos.enable-patching", "true"));
    assertFalse(props.getDatasource().isMigrate());
    assertTrue(props.isEnablePatching());
  }

  @Test
  void bindsConductorProperties() {
    var props = bind(Map.of("dbos.conductor.key", "my-key", "dbos.conductor.domain", "my-domain"));
    assertEquals("my-key", props.getConductor().getKey());
    assertEquals("my-domain", props.getConductor().getDomain());
  }

  @Test
  void bindsIdentityProperties() {
    var props = bind(Map.of("dbos.executor-id", "exec-1"));
    assertEquals("exec-1", props.getExecutorId());
  }

  @Test
  void bindsDatasourceSchema() {
    var props = bind(Map.of("dbos.datasource.schema", "my_schema"));
    assertEquals("my_schema", props.getDatasource().getSchema());
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
