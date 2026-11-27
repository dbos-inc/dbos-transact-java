package dev.dbos.transact.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSExecutorTestAccess;
import dev.dbos.transact.spi.EnrolledWorkflow;
import dev.dbos.transact.spi.TestEnterpriseProvider;
import dev.dbos.transact.spi.TestEnterpriseProvider.RecordingAutoUpgradePass;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.AutoUpgrade;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

interface UpgradeJobs {
  void optedIn();

  void optedOut();

  void inherits();
}

@WorkflowClassName("UpgradeJobs")
class UpgradeJobsImpl implements UpgradeJobs {
  @Workflow(autoUpgrade = AutoUpgrade.ENABLED)
  @Override
  public void optedIn() {}

  @Workflow(autoUpgrade = AutoUpgrade.DISABLED)
  @Override
  public void optedOut() {}

  @Workflow
  @Override
  public void inherits() {}
}

interface OtherUpgradeJobs {
  void optedIn();
}

// The same workflow name as in UpgradeJobs, in another class, which opts out.
@WorkflowClassName("OtherUpgradeJobs")
class OtherUpgradeJobsImpl implements OtherUpgradeJobs {
  @Workflow(autoUpgrade = AutoUpgrade.DISABLED)
  @Override
  public void optedIn() {}
}

interface OptedInJob {
  void wantsUpgrade();
}

@WorkflowClassName("OptedInJob")
class OptedInJobImpl implements OptedInJob {
  @Workflow(autoUpgrade = AutoUpgrade.ENABLED)
  @Override
  public void wantsUpgrade() {}
}

interface OptedOutJob {
  void optedOut();
}

@WorkflowClassName("OptedOutJob")
class OptedOutJobImpl implements OptedOutJob {
  @Workflow(autoUpgrade = AutoUpgrade.DISABLED)
  @Override
  public void optedOut() {}
}

interface InheritingJob {
  void inherits();
}

@WorkflowClassName("InheritingJob")
class InheritingJobImpl implements InheritingJob {
  @Workflow
  @Override
  public void inherits() {}
}

interface HeldJob {
  void holds();
}

// Runs until released, so a shutdown's wait has a workflow to wait for.
@WorkflowClassName("HeldJob")
class HeldJobImpl implements HeldJob {
  private final CountDownLatch started;
  private final CountDownLatch release;

  HeldJobImpl(CountDownLatch started, CountDownLatch release) {
    this.started = started;
    this.release = release;
  }

  @Workflow
  @Override
  public void holds() {
    started.countDown();
    try {
      release.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}

/**
 * Auto-upgrade wiring: who enrolls, when launch refuses, which key the entitlement is checked with,
 * and the loop's life from launch to shutdown. The enterprise artifact is stood in for by {@link
 * TestEnterpriseProvider}, and Conductor's entitlements route by a local server.
 */
@ResourceLock("Entitlements log")
// Its loops write to the log whose warnings AutoUpgradeLoopTest counts.
@ResourceLock(AutoUpgradeLoopTest.LOOP_LOG)
public class AutoUpgradeLaunchTest {
  private static final String ENTITLEMENTS = "/v2/entitlements";
  private static final Duration INTERVAL = Duration.ofMillis(50);
  private static final Duration GRACE = Duration.ofMillis(7500);

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;

  private EntitlementsTest.FakeConductor conductor;

  @BeforeEach
  void startConductor() throws IOException {
    conductor = new EntitlementsTest.FakeConductor();
    conductor.entitle("{\"autoUpgrade\": true}");
  }

  @AfterEach
  void stopConductor() {
    conductor.close();
  }

  /** A config with a Conductor key, a self-hosted Conductor, and short auto-upgrade timings. */
  private DBOSConfig conductorConfig(String appName) {
    return pgContainer
        .dbosConfig(appName)
        .withConductorKey("test-key")
        .withConductorDomain(conductor.url)
        .withAutoUpgradeInterval(INTERVAL)
        .withAutoUpgradeGrace(GRACE);
  }

  private List<Map.Entry<String, String>> checks() {
    return conductor.requests.stream().map(e -> Map.entry(e.getKey(), e.getValue())).toList();
  }

  private static List<RecordingAutoUpgradePass> passesOf(String appName) {
    return TestEnterpriseProvider.autoUpgradePasses().stream()
        .filter(upgradePass -> upgradePass.engine.appName().equals(appName))
        .toList();
  }

  private static AutoUpgradeLoop loopOf(DBOS dbos) {
    return DBOSExecutorTestAccess.getAutoUpgradeLoop(DBOSTestAccess.getDbosExecutor(dbos));
  }

  private static Set<EnrolledWorkflow> enrolled(DBOS dbos) {
    return new HashSet<>(DBOSTestAccess.getDbosExecutor(dbos).autoUpgradableWorkflows());
  }

  private void assertNotLaunched() {
    assertNull(DBOSTestAccess.getDbosExecutor(dbos));
  }

  // --- Gating

  @Test
  void autoUpgradeIsAnEntitledFeatureOnlyWhenOn() {
    var config = DBOSConfig.defaults("app");

    assertEquals(List.of(), Entitlements.enabledFeatures(config));
    assertEquals(List.of(), Entitlements.enabledFeatures(config.withAutoUpgrade(false)));
    assertEquals(
        List.of(Entitlements.AUTO_UPGRADE),
        Entitlements.enabledFeatures(config.withAutoUpgrade(true)));
    assertEquals(
        List.of(Entitlements.AUTO_UPGRADE, Entitlements.SUSPEND_WORKFLOWS),
        Entitlements.enabledFeatures(config.withAutoUpgrade(true).withSuspendMode(true)));
    assertEquals("autoUpgrade", Entitlements.AUTO_UPGRADE.entitlement());
    assertEquals("Auto-upgrading workflows", Entitlements.AUTO_UPGRADE.description());
  }

  @Test
  void autoUpgradeRequiresAKey() {
    var app = "upgrade-requires-a-key";
    dbos = new DBOS(pgContainer.dbosConfig(app).withAutoUpgrade(true));
    dbos.registerProxy(InheritingJob.class, new InheritingJobImpl());

    var refused = assertThrows(IllegalStateException.class, dbos::launch);
    assertTrue(
        refused.getMessage().startsWith("Auto-upgrading workflows requires a DBOS key"),
        refused.getMessage());
    assertNotLaunched();
    assertEquals(List.of(), passesOf(app));

    // The refused launch leaves nothing behind.
    try (var plain = new DBOS(pgContainer.dbosConfig(app))) {
      plain.registerProxy(InheritingJob.class, new InheritingJobImpl());
      plain.launch();
      assertNull(loopOf(plain));
    }
  }

  @Test
  void anOptedInWorkflowRequiresAKeyAtLaunch() {
    var app = "opted-in-requires-a-key";
    var config = pgContainer.dbosConfig(app);
    // The config is off and no key is given, so building the executor checks nothing...
    assertDoesNotThrow(() -> new DBOSExecutor(config));
    dbos = new DBOS(config);
    dbos.registerProxy(OptedInJob.class, new OptedInJobImpl());

    // ...but the workflow's opt-in needs a key, which only the launch can tell.
    var refused = assertThrows(IllegalStateException.class, dbos::launch);
    assertTrue(
        refused.getMessage().startsWith("Auto-upgrading workflows requires a DBOS key"),
        refused.getMessage());
    assertNotLaunched();
    assertEquals(List.of(), checks());
    assertEquals(List.of(), passesOf(app));
  }

  @Test
  void launchRefusesWhenTheConfigFlagIsNotEntitled() {
    var app = "flag-not-entitled";
    conductor.entitle("{\"autoUpgrade\": false}");
    var config = conductorConfig(app).withAutoUpgrade(true);

    // The config turns the feature on, so building the executor checks it, before any state
    // exists.
    var refused = assertThrows(IllegalStateException.class, () -> new DBOSExecutor(config));
    assertTrue(refused.getMessage().contains("not entitled to autoUpgrade"), refused.getMessage());
    assertTrue(refused.getMessage().contains("plan does not include it"), refused.getMessage());
    assertEquals(List.of(Map.entry(ENTITLEMENTS, "Bearer test-key")), checks());

    dbos = new DBOS(config);
    dbos.registerProxy(InheritingJob.class, new InheritingJobImpl());
    assertThrows(IllegalStateException.class, dbos::launch);
    assertNotLaunched();
    assertEquals(List.of(), passesOf(app));

    // The refused launch leaves nothing behind.
    try (var plain = new DBOS(pgContainer.dbosConfig(app))) {
      plain.launch();
    }
  }

  @Test
  void launchRefusesWhenAnOptedInWorkflowIsNotEntitled() {
    var app = "opt-in-not-entitled";
    var config = conductorConfig(app);
    // The config is off, so building the executor checks nothing...
    assertDoesNotThrow(() -> new DBOSExecutor(config));
    assertEquals(List.of(), checks());

    dbos = new DBOS(config);
    dbos.registerProxy(OptedInJob.class, new OptedInJobImpl());

    // ...and the workflow's opt-in is checked at launch, which refuses.
    conductor.entitle("{\"autoUpgrade\": false}");
    var refused = assertThrows(IllegalStateException.class, dbos::launch);
    assertTrue(refused.getMessage().contains("plan does not include it"), refused.getMessage());
    assertEquals(List.of(Map.entry(ENTITLEMENTS, "Bearer test-key")), checks());
    assertNotLaunched();
    assertEquals(List.of(), passesOf(app));
  }

  @Test
  void anAutoUpgradingExecutorChecksItsEntitlement() {
    var app = "checks-its-entitlement";
    conductor.entitle("{\"suspendWorkflows\": true, \"autoUpgrade\": false}");
    dbos =
        new DBOS(
            pgContainer
                .dbosConfig(app)
                .withAutoUpgrade(true)
                .withTransactEnterpriseKey("enterprise-key")
                .withConductorDomain(conductor.url));

    var refused = assertThrows(IllegalStateException.class, dbos::launch);
    assertTrue(refused.getMessage().contains("not entitled to autoUpgrade"), refused.getMessage());
    assertEquals(List.of(Map.entry(ENTITLEMENTS, "Bearer enterprise-key")), checks());
  }

  @Test
  void theEntitlementIsCheckedWithTheEnterpriseKeyFirst() {
    // Both keys given: the enterprise key is the one checked.
    try (var both =
        new DBOS(
            conductorConfig("enterprise-key-first")
                .withAutoUpgrade(true)
                .withTransactEnterpriseKey("enterprise-key"))) {
      both.registerProxy(InheritingJob.class, new InheritingJobImpl());
      both.launch();

      // Checked once, with the config: the launch does not ask again for the config flag. The
      // self-hosted Conductor configured is the one asked.
      assertEquals(List.of(Map.entry(ENTITLEMENTS, "Bearer enterprise-key")), checks());
      assertNotNull(loopOf(both));
    }

    // The enterprise key alone is enough: no Conductor connection is needed.
    dbos =
        new DBOS(
            pgContainer
                .dbosConfig("enterprise-key-alone")
                .withAutoUpgrade(true)
                .withTransactEnterpriseKey("enterprise-key")
                .withConductorDomain(conductor.url)
                .withAutoUpgradeInterval(INTERVAL));
    dbos.registerProxy(InheritingJob.class, new InheritingJobImpl());
    dbos.launch();

    assertEquals(
        List.of(
            Map.entry(ENTITLEMENTS, "Bearer enterprise-key"),
            Map.entry(ENTITLEMENTS, "Bearer enterprise-key")),
        checks());
    assertNull(DBOSExecutorTestAccess.getConductorOptions(DBOSTestAccess.getDbosExecutor(dbos)));
    assertNotNull(loopOf(dbos));
    assertTrue(loopOf(dbos).isAlive());
  }

  // --- Enrollment

  @Test
  void workflowsEnrollByFlagThenByConfig() {
    var optedIn = new EnrolledWorkflow("UpgradeJobs", "optedIn");
    var inherits = new EnrolledWorkflow("UpgradeJobs", "inherits");

    // The config is off, so only the explicit opt-in enrolls: by its class and its name, so the
    // workflow of the same name in the other class stays out.
    try (var off = new DBOS(conductorConfig("enroll-config-off"))) {
      off.registerProxy(UpgradeJobs.class, new UpgradeJobsImpl());
      off.registerProxy(OtherUpgradeJobs.class, new OtherUpgradeJobsImpl());
      off.launch();
      assertEquals(Set.of(optedIn), enrolled(off));
    }

    // The config is on: every workflow enrolls unless it opted out. A class registered under two
    // instance names enrolls its workflows once.
    dbos = new DBOS(conductorConfig("enroll-config-on").withAutoUpgrade(true));
    dbos.registerProxy(UpgradeJobs.class, new UpgradeJobsImpl());
    dbos.registerProxy(UpgradeJobs.class, new UpgradeJobsImpl(), "second");
    dbos.registerProxy(OtherUpgradeJobs.class, new OtherUpgradeJobsImpl());
    dbos.launch();
    assertEquals(Set.of(optedIn, inherits), enrolled(dbos));
    assertEquals(2, DBOSTestAccess.getDbosExecutor(dbos).autoUpgradableWorkflows().size());
  }

  @Test
  void noLoopWithoutEnrolledWorkflows() {
    var app = "nothing-enrolled";
    // A key is given, but nothing enrolls: the config is off and the one workflow opts out.
    dbos = new DBOS(conductorConfig(app));
    dbos.registerProxy(OptedOutJob.class, new OptedOutJobImpl());
    dbos.launch();

    assertNull(loopOf(dbos));
    assertEquals(List.of(), passesOf(app));
    // So no entitlement is checked either.
    assertEquals(List.of(), checks());
  }

  // --- The loop's life through DBOS

  @Test
  void theLoopRunsThePassUntilShutdown() throws Exception {
    var app = "loop-until-shutdown";
    dbos = new DBOS(conductorConfig(app).withAutoUpgrade(true));
    dbos.registerProxy(InheritingJob.class, new InheritingJobImpl());
    dbos.launch();

    // The config's flag reaches the executor: the unflagged workflow enrolls, and the Conductor
    // key stands in for the enterprise key.
    assertEquals(Set.of(new EnrolledWorkflow("InheritingJob", "inherits")), enrolled(dbos));
    assertEquals(List.of(Map.entry(ENTITLEMENTS, "Bearer test-key")), checks());
    var loop = loopOf(dbos);
    assertNotNull(loop);
    assertTrue(loop.isAlive());
    assertEquals(INTERVAL, loop.interval());

    // The pass was built for this executor with the configured grace, and runs on the configured
    // interval.
    var passes = passesOf(app);
    assertEquals(1, passes.size());
    var upgradePass = passes.get(0);
    assertSame(upgradePass, loop.upgradePass());
    assertEquals(GRACE, upgradePass.grace);
    assertEquals(
        List.of(new EnrolledWorkflow("InheritingJob", "inherits")),
        upgradePass.engine.autoUpgradableWorkflows());
    AutoUpgradeLoopTest.awaitRuns(upgradePass, 3);
    // Every pass is given the one stop signal.
    var stopSignal = upgradePass.runs.get(0);
    assertTrue(upgradePass.runs.stream().allMatch(signal -> signal == stopSignal));
    assertFalse(stopSignal.getAsBoolean());

    dbos.shutdown();

    assertFalse(loop.isAlive());
    assertTrue(stopSignal.getAsBoolean());
    assertEquals(1, upgradePass.closes.get());
    // Nothing runs after shutdown.
    var runs = upgradePass.runs.size();
    Thread.sleep(200);
    assertEquals(runs, upgradePass.runs.size());
  }

  @Test
  void theDefaultIntervalAndGrace() {
    var app = "default-interval";
    dbos =
        new DBOS(
            conductorConfig(app)
                .withAutoUpgrade(true)
                .withAutoUpgradeInterval(null)
                .withAutoUpgradeGrace(null));
    dbos.registerProxy(InheritingJob.class, new InheritingJobImpl());
    dbos.launch();

    assertNotNull(loopOf(dbos));
    assertEquals(Duration.ofSeconds(30), loopOf(dbos).interval());
    var passes = passesOf(app);
    assertEquals(1, passes.size());
    assertEquals(Duration.ZERO, passes.get(0).grace);
  }

  @Test
  void shutdownStopsTheLoopAfterTheWaitAndBeforeTheBackgroundJobs() throws Exception {
    var app = "loop-stops-after-the-wait";
    dbos = new DBOS(conductorConfig(app).withAutoUpgrade(true));
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var job = dbos.registerProxy(HeldJob.class, new HeldJobImpl(started, release));
    dbos.launch();
    var loop = loopOf(dbos);
    var upgradePass = passesOf(app).get(0);
    var jobsThread =
        DBOSExecutorTestAccess.backgroundJobsThread(DBOSTestAccess.getDbosExecutor(dbos));
    // What is still up when the stopped loop closes its pass: (loop alive, workflow still held,
    // background jobs running, Conductor connection stopped).
    var seenAtClose = new CopyOnWriteArrayList<List<Object>>();
    upgradePass.onClose =
        () ->
            seenAtClose.add(
                List.of(
                    loop.isAlive(),
                    release.getCount() > 0,
                    jobsThread.isAlive(),
                    TestEnterpriseProvider.conductorStopped(app)));
    dbos.startWorkflow(() -> job.holds());
    assertTrue(started.await(10, TimeUnit.SECONDS));

    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var shutdown = CompletableFuture.runAsync(() -> dbos.shutdown(Duration.ofSeconds(30)));
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!executor.isClosing()) {
      assertTrue(System.nanoTime() < deadline, "the shutdown never began");
      Thread.sleep(10);
    }

    // The loop keeps running passes through the wait.
    AutoUpgradeLoopTest.awaitRuns(upgradePass, upgradePass.runs.size() + 2);
    assertTrue(loop.isAlive());
    assertEquals(0, upgradePass.closes.get());

    release.countDown();
    shutdown.get(30, TimeUnit.SECONDS);

    // The loop stopped once the workflow had finished, before the background jobs and the
    // Conductor connection.
    assertEquals(List.of(List.<Object>of(false, false, true, false)), seenAtClose);
    assertEquals(1, upgradePass.closes.get());
    assertFalse(jobsThread.isAlive());
    assertTrue(TestEnterpriseProvider.conductorStopped(app));
  }

  @Test
  void theEngineSaysWhetherThisProcessHoldsTheBackgroundJobsLock() throws Exception {
    var app = "engine-holds-the-lock";
    dbos = new DBOS(conductorConfig(app).withAutoUpgrade(true));
    dbos.registerProxy(InheritingJob.class, new InheritingJobImpl());
    dbos.launch();
    var engine = passesOf(app).get(0).engine;
    var executor = DBOSTestAccess.getDbosExecutor(dbos);

    // The only process of its version takes the lock.
    DBOSExecutorTestAccess.awaitBackgroundLease(executor);
    assertTrue(engine.holdsBackgroundJobsLock());

    // A graceful shutdown waits for the background jobs, which give the lock up as they stop.
    dbos.shutdown(Duration.ofSeconds(10));
    assertFalse(engine.holdsBackgroundJobsLock());
  }
}
