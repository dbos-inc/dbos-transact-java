package dev.dbos.transact.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.spi.TestEnterpriseProvider;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

interface SuspendFlagService {
  String recvWithSuspend();

  String sleepWithSuspend();

  String sleepWithSuspendInAStep();
}

class SuspendFlagServiceImpl implements SuspendFlagService {
  final DBOS dbos;

  SuspendFlagServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Workflow
  @Override
  public String recvWithSuspend() {
    return dbos.<String>recv("topic", Duration.ofSeconds(1), true).orElse("none");
  }

  @Workflow
  @Override
  public String sleepWithSuspend() {
    dbos.sleep(Duration.ofSeconds(1), true);
    return "slept";
  }

  @Workflow
  @Override
  public String sleepWithSuspendInAStep() {
    dbos.runStep(() -> dbos.sleep(Duration.ofSeconds(1), true), "sleep");
    return "slept";
  }
}

/**
 * Suspend mode is a Transact Enterprise feature: turning it on checks the entitlement, and builds
 * suspend mode through the one enterprise provider. Here that is {@link TestEnterpriseProvider};
 * {@code EnterpriseProvidersTest} covers the error when no provider is on the class path.
 */
public class SuspendModeLaunchTest {
  // Only this class builds suspend mode, so a lock here keeps the provider's count steady.
  private static final String SUSPEND_BACKENDS_CREATED =
      "TestEnterpriseProvider.suspendBackendsCreated";

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @Test
  void suspendModeIsAnEntitledFeatureOnlyWhenOn() {
    var config = DBOSConfig.defaults("app");

    assertEquals(List.of(), Entitlements.enabledFeatures(config));
    assertEquals(
        List.of(Entitlements.SUSPEND_WORKFLOWS),
        Entitlements.enabledFeatures(config.withSuspendMode(true)));
    assertEquals("suspendWorkflows", Entitlements.SUSPEND_WORKFLOWS.entitlement());
  }

  @Test
  void suspendModeIsOffByDefaultAndIsNotLeakedByToString() {
    var config = DBOSConfig.defaults("app");

    assertFalse(config.suspendMode());
    assertTrue(config.withSuspendMode(true).suspendMode());
    assertTrue(config.withSuspendMode(true).toString().contains("suspendMode=true"));
    // The setting survives the other withers.
    assertTrue(config.withSuspendMode(true).withAppName("renamed").suspendMode());
  }

  @Test
  @ResourceLock("Entitlements log")
  @ResourceLock(SUSPEND_BACKENDS_CREATED)
  void launchWithSuspendModeBuildsSuspendThroughTheProvider() throws Exception {
    try (var conductor = new EntitlementsTest.FakeConductor()) {
      conductor.entitle("{\"suspendWorkflows\": true}");
      var config =
          pgContainer.dbosConfig().withConductorDomain(conductor.url).withSuspendMode(true);
      var suspends = TestEnterpriseProvider.suspendBackendsCreated();

      try (var dbos = new DBOS(config)) {
        dbos.launch();
      }

      assertEquals(suspends + 1, TestEnterpriseProvider.suspendBackendsCreated());
    }
  }

  @Test
  @ResourceLock(SUSPEND_BACKENDS_CREATED)
  void launchWithoutSuspendModeBuildsNoSuspend() {
    var suspends = TestEnterpriseProvider.suspendBackendsCreated();

    try (var dbos = new DBOS(pgContainer.dbosConfig())) {
      dbos.launch();
    }

    assertEquals(suspends, TestEnterpriseProvider.suspendBackendsCreated());
  }

  @Test
  @ResourceLock("Entitlements log")
  void launchWithSuspendModeNeedsTheEntitlement() throws Exception {
    try (var conductor = new EntitlementsTest.FakeConductor()) {
      conductor.entitle("{\"suspendWorkflows\": false}");
      var config =
          pgContainer.dbosConfig().withConductorDomain(conductor.url).withSuspendMode(true);

      var thrown =
          assertThrows(
              IllegalStateException.class,
              () -> {
                try (var dbos = new DBOS(config)) {
                  dbos.launch();
                }
              });

      assertTrue(thrown.getMessage().contains("suspendWorkflows"), thrown.getMessage());
    }
  }

  @Test
  void suspendOnRecvAndSleepIsRefusedWithoutSuspendMode() {
    try (var dbos = new DBOS(pgContainer.dbosConfig())) {
      var service = dbos.registerProxy(SuspendFlagService.class, new SuspendFlagServiceImpl(dbos));
      dbos.launch();

      for (var call :
          List.<java.util.function.Supplier<String>>of(
              service::recvWithSuspend, service::sleepWithSuspend)) {
        var thrown = assertThrows(IllegalStateException.class, call::get);
        assertTrue(thrown.getMessage().contains("requires suspend mode"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("Transact Enterprise"), thrown.getMessage());
      }
    }
  }

  @Test
  void suspendOnSleepIsRefusedOutsideAWorkflowAndInAStep() {
    try (var dbos = new DBOS(pgContainer.dbosConfig())) {
      var service = dbos.registerProxy(SuspendFlagService.class, new SuspendFlagServiceImpl(dbos));
      dbos.launch();

      // Without suspend, both sleep in the thread; with it, there is no workflow to suspend.
      var outside =
          assertThrows(IllegalStateException.class, () -> dbos.sleep(Duration.ofSeconds(1), true));
      assertTrue(outside.getMessage().contains("from within a workflow"), outside.getMessage());

      var inStep = assertThrows(IllegalStateException.class, service::sleepWithSuspendInAStep);
      assertTrue(inStep.getMessage().contains("within a step"), inStep.getMessage());
    }
  }
}
