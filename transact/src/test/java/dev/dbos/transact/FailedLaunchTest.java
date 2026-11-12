package dev.dbos.transact;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.execution.DBOSLifecycleListener;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A launch that throws leaves the instance as if it had never been launched: a later launch tries
 * again from the start, and other calls fail as they do before any launch.
 */
public class FailedLaunchTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;

  private FailedLaunchService proxy;

  private void create(DBOSConfig config) {
    dbos = new DBOS(config);
    proxy = dbos.registerProxy(FailedLaunchService.class, new FailedLaunchServiceImpl());
  }

  private void assertNotLaunched() {
    var e = assertThrows(IllegalStateException.class, () -> dbos.getWorkflowStatus("any"));
    assertTrue(e.getMessage().contains("before DBOS is launched"), e.getMessage());
  }

  // Migrates a schema from empty, which takes minutes on a slow CockroachDB runner, as in
  // CustomSchemaTest.
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void aLaunchRefusedForAnUnmigratedSchemaCanBeRetriedOnceItIsMigrated() throws Exception {
    // A schema no migration has run on, which a launch with migrations off must refuse.
    var schema = "failed_launch_%d".formatted(System.nanoTime());
    var config = pgContainer.dbosConfig().withDatabaseSchema(schema).withMigrate(false);
    create(config);

    var first = assertThrows(IllegalStateException.class, dbos::launch);
    assertNotLaunched();
    // Tried again for real, not skipped because an executor is already in place.
    var second = assertThrows(IllegalStateException.class, dbos::launch);
    assertEquals(first.getMessage(), second.getMessage());

    MigrationManager.runMigrations(config.withMigrate(true));
    dbos.launch();
    var handle =
        dbos.startWorkflow(() -> proxy.echo("after"), new StartWorkflowOptions("echo-after"));
    assertEquals("after", handle.getResult());
  }

  @Test
  public void aLaunchThatFailsPartWayShutsDownWhatItStartedAndCanBeRetried() throws Exception {
    create(pgContainer.dbosConfig());
    var listener = new FailingOnceListener();
    dbos.integration().registerLifecycleListener(listener);

    var e = assertThrows(IllegalStateException.class, dbos::launch);
    assertEquals("listener failed", e.getMessage());
    assertEquals(1, listener.shutDowns.get(), "what the failed launch started was shut down");
    assertNotLaunched();

    dbos.launch();
    assertEquals(2, listener.launches.get());
    var handle = dbos.startWorkflow(() -> proxy.echo("again"), new StartWorkflowOptions("again"));
    assertEquals("again", handle.getResult());
  }

  /** Fails the first launch, after the executor has started its services, and not the next. */
  private static final class FailingOnceListener implements DBOSLifecycleListener {
    final AtomicInteger launches = new AtomicInteger();
    final AtomicInteger shutDowns = new AtomicInteger();

    @Override
    public void dbosLaunched(DBOS dbos) {
      if (launches.incrementAndGet() == 1) {
        throw new IllegalStateException("listener failed");
      }
    }

    @Override
    public void dbosShutDown() {
      shutDowns.incrementAndGet();
    }
  }
}

interface FailedLaunchService {
  String echo(String value);
}

class FailedLaunchServiceImpl implements FailedLaunchService {
  @Override
  @Workflow
  public String echo(String value) {
    return value;
  }
}
