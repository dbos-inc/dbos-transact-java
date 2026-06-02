package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExternalStateTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose SystemDatabase systemDatabase;
  DBOSConfig dbosConfig;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);

    systemDatabase = SystemDatabase.create(dbosConfig);
  }

  @Test
  public void externalStateTime() {
    var service = "test-service-name";
    var workflow = "test-workflow-name";
    var key = "externalStateTime-key";
    var now = Instant.now();
    var value = "%d".formatted(now.toEpochMilli());

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
    var pastNow = now.minus(Duration.ofMillis(10));
    var pastValue = "%d".formatted(pastNow.toEpochMilli());

    var upState =
        systemDatabase.upsertExternalState(
            new ExternalState(service, workflow, key).withValue(pastValue).withUpdateTime(pastNow));
    assertEquals(service, upState.service());
    assertEquals(workflow, upState.workflowName());
    assertEquals(key, upState.key());
    assertEquals(value, upState.value());
    assertEquals(now, upState.updateTime());
    assertNull(upState.updateSeq());

    // upsert later timestamp does change the value
    var futureNow = now.plus(Duration.ofMillis(10));
    var futureValue = "%d".formatted(futureNow.toEpochMilli());

    upState =
        systemDatabase.upsertExternalState(
            new ExternalState(service, workflow, key)
                .withValue(futureValue)
                .withUpdateTime(futureNow));
    assertEquals(service, upState.service());
    assertEquals(workflow, upState.workflowName());
    assertEquals(key, upState.key());
    assertEquals(futureValue, upState.value());
    assertEquals(futureNow, upState.updateTime());
    assertNull(upState.updateSeq());
  }

  @Test
  public void externalStateSeq() {
    var service = "test-service-name";
    var workflow = "test-workflow-name";
    var key = "externalStateSeq-key";
    BigInteger seq = BigInteger.valueOf(10);
    var value = "%d".formatted(seq.longValue());

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
    var oldSeq = seq.subtract(BigInteger.valueOf(1));
    var oldValue = "%d".formatted(oldSeq.longValue());
    state =
        systemDatabase.upsertExternalState(
            new ExternalState(service, workflow, key).withValue(oldValue).withUpdateSeq(oldSeq));
    assertEquals(service, state.service());
    assertEquals(workflow, state.workflowName());
    assertEquals(key, state.key());
    assertEquals(value, state.value());
    assertEquals(seq, state.updateSeq());
    assertNull(state.updateTime());

    // upsert later timestamp does change the value
    var newSeq = seq.add(BigInteger.valueOf(1));
    var newValue = "%d".formatted(newSeq.longValue());
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
