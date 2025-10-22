package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class ExternalStateTest {
  private static SystemDatabase systemDatabase;
  private static DBOSConfig dbosConfig;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withDbUser("postgres");

    DBUtils.recreateDB(dbosConfig);
    MigrationManager.runMigrations(dbosConfig);
    systemDatabase = new SystemDatabase(dbosConfig);
  }

  @AfterAll
  static void onetimeTearDown() throws Exception {
    systemDatabase.close();
  }

  @Test
  public void externalStateTime() {
    var service = "test-service-name";
    var workflow = "test-workflow-name";
    var key = "externalStateTime-key";
    var nowMS = System.currentTimeMillis();
    var value = "%d".formatted(nowMS);
    var now = BigDecimal.valueOf(nowMS).setScale(15);

    // insert initial value
    var insState =
        systemDatabase.upsertExternalState(
            new ExternalState(service, workflow, key).withValue(value).withUpdateTime(now));
    assertEquals(service, insState.service());
    assertEquals(workflow, insState.workflowName());
    assertEquals(key, insState.key());
    assertEquals(value, insState.value());
    assertEquals(now, insState.updateTime());
    assertNull(insState.updateSeq());

    // ensure upserted value can be retrieved
    var getState = systemDatabase.getExternalState(service, workflow, key);
    assertTrue(getState.isPresent());
    assertEquals(service, getState.get().service());
    assertEquals(workflow, getState.get().workflowName());
    assertEquals(key, getState.get().key());
    assertEquals(value, getState.get().value());
    assertEquals(now, getState.get().updateTime());
    assertNull(getState.get().updateSeq());

    // upsert older timestamp doesn't change the value
    var newNowMS = nowMS - 10;
    var newValue = "%d".formatted(newNowMS);
    var newNow = BigDecimal.valueOf(newNowMS).setScale(15);

    var upState =
        systemDatabase.upsertExternalState(
            new ExternalState(service, workflow, key).withValue(newValue).withUpdateTime(newNow));
    assertEquals(service, upState.service());
    assertEquals(workflow, upState.workflowName());
    assertEquals(key, upState.key());
    assertEquals(value, upState.value());
    assertEquals(now, upState.updateTime());
    assertNull(upState.updateSeq());

    // upsert later timestamp does change the value
    newNowMS = nowMS + 10;
    newValue = "%d".formatted(newNowMS);
    newNow = BigDecimal.valueOf(newNowMS).setScale(15);

    upState =
        systemDatabase.upsertExternalState(
            new ExternalState(service, workflow, key).withValue(newValue).withUpdateTime(newNow));
    assertEquals(service, upState.service());
    assertEquals(workflow, upState.workflowName());
    assertEquals(key, upState.key());
    assertEquals(newValue, upState.value());
    assertEquals(newNow, upState.updateTime());
    assertNull(upState.updateSeq());
  }

  @Test
  public void externalStateSeq() {
    var service = "test-service-name";
    var workflow = "test-workflow-name";
    var key = "externalStateSeq-key";
    var nowMS = System.currentTimeMillis();
    var value = "%d".formatted(nowMS);
    var seq = BigInteger.valueOf(nowMS);

    // insert initial value
    var state =
        systemDatabase.upsertExternalState(
            new ExternalState(service, workflow, key).withValue(value).withUpdateSeq(seq));
    assertEquals(service, state.service());
    assertEquals(workflow, state.workflowName());
    assertEquals(key, state.key());
    assertEquals(value, state.value());
    assertEquals(seq, state.updateSeq());
    assertNull(state.updateTime());

    // ensure upserted value can be retrieved
    var getState = systemDatabase.getExternalState(service, workflow, key);
    assertTrue(getState.isPresent());
    assertEquals(service, getState.get().service());
    assertEquals(workflow, getState.get().workflowName());
    assertEquals(key, getState.get().key());
    assertEquals(value, getState.get().value());
    assertEquals(seq, getState.get().updateSeq());
    assertNull(getState.get().updateTime());

    // upsert older timestamp doesn't change the value
    var newNowMS = nowMS - 10;
    var newValue = "%d".formatted(newNowMS);
    var newSeq = BigInteger.valueOf(newNowMS);

    state =
        systemDatabase.upsertExternalState(
            new ExternalState(service, workflow, key).withValue(newValue).withUpdateSeq(newSeq));
    assertEquals(service, state.service());
    assertEquals(workflow, state.workflowName());
    assertEquals(key, state.key());
    assertEquals(value, state.value());
    assertEquals(seq, state.updateSeq());
    assertNull(state.updateTime());

    // upsert later timestamp does change the value
    newNowMS = nowMS + 10;
    newValue = "%d".formatted(newNowMS);
    newSeq = BigInteger.valueOf(newNowMS);

    state =
        systemDatabase.upsertExternalState(
            new ExternalState(service, workflow, key).withValue(newValue).withUpdateSeq(newSeq));
    assertEquals(service, state.service());
    assertEquals(workflow, state.workflowName());
    assertEquals(key, state.key());
    assertEquals(newValue, state.value());
    assertEquals(newSeq, state.updateSeq());
    assertNull(state.updateTime());
  }
}
