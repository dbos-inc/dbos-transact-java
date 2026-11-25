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
import dev.dbos.transact.execution.DBOSLifecycleListener;
import dev.dbos.transact.spi.EnrolledWorkflow;
import dev.dbos.transact.spi.TestEnterpriseProvider;
import dev.dbos.transact.spi.TestEnterpriseProvider.RecordingAutoUpgradePass;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.AutoUpgrade;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

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
}
