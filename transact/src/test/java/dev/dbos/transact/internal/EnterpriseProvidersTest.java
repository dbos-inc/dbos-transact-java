package dev.dbos.transact.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.conductor.ConductorConnection;
import dev.dbos.transact.conductor.ConductorOptions;
import dev.dbos.transact.spi.EnterpriseEngine;
import dev.dbos.transact.spi.EnterpriseProvider;
import dev.dbos.transact.spi.SuspendBackend;
import dev.dbos.transact.spi.TestEnterpriseProvider;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class EnterpriseProvidersTest {

  @Test
  public void loadsTheRegisteredProvider() {
    assertInstanceOf(
        TestEnterpriseProvider.class, EnterpriseProviders.load(EnterpriseProviders.CONDUCTOR));
  }

  @Test
  public void aContextLoaderThatDelegatesToDbosFindsItOnce() throws Exception {
    // The child sees the test registration through its parent; finding it twice would read as
    // two providers.
    var thread = Thread.currentThread();
    var previous = thread.getContextClassLoader();
    try (var child = new URLClassLoader(new URL[0], EnterpriseProvider.class.getClassLoader())) {
      thread.setContextClassLoader(child);
      assertInstanceOf(
          TestEnterpriseProvider.class, EnterpriseProviders.load(EnterpriseProviders.CONDUCTOR));
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  @Test
  public void selectsTheOnlyProvider() {
    var provider = new TestEnterpriseProvider();
    assertSame(
        provider,
        EnterpriseProviders.select(
            List.of(provider), "1.3.0", EnterpriseProviders.CONDUCTOR, false));
  }

  @Test
  public void noProviderNamesTheReleaseToAdd() {
    var e =
        assertThrows(
            IllegalStateException.class,
            () ->
                EnterpriseProviders.select(
                    List.of(), "1.3.0", EnterpriseProviders.CONDUCTOR, false));
    assertEquals(
        "Connecting to DBOS Conductor requires dev.dbos:transact-enterprise on the class path."
            + " Add dev.dbos:transact-enterprise:1.3.0, the same version as dev.dbos:transact",
        e.getMessage());
  }

  @Test
  public void noProviderOnAPreviewNamesTheMinor() {
    var e =
        assertThrows(
            IllegalStateException.class,
            () ->
                EnterpriseProviders.select(
                    List.of(), "1.3.0-m25", EnterpriseProviders.CONDUCTOR, false));
    assertEquals(
        "Connecting to DBOS Conductor requires dev.dbos:transact-enterprise on the class path."
            + " Add a dev.dbos:transact-enterprise 1.3 build, as dev.dbos:transact is 1.3.0-m25",
        e.getMessage());
  }

  @Test
  public void moreThanOneProviderIsRejected() {
    var providers =
        List.<EnterpriseProvider>of(new TestEnterpriseProvider(), new TestEnterpriseProvider());
    assertThrows(
        IllegalStateException.class,
        () -> EnterpriseProviders.select(providers, "1.3.0", EnterpriseProviders.CONDUCTOR, false));
  }

  @Test
  public void noProviderNamesTheFeatureThatNeedsIt() {
    var e =
        assertThrows(
            IllegalStateException.class,
            () ->
                EnterpriseProviders.select(
                    List.of(), "1.4.0", "Suspending workflows on recv and sleep", false));
    assertEquals(
        "Suspending workflows on recv and sleep requires dev.dbos:transact-enterprise on the class"
            + " path. Add dev.dbos:transact-enterprise:1.4.0, the same version as"
            + " dev.dbos:transact",
        e.getMessage());
  }

  @Test
  public void aProviderBuiltBeforeAutoUpgradeSaysSo() {
    var provider =
        new EnterpriseProvider() {
          @Override
          public ConductorConnection createConductor(
              EnterpriseEngine engine, ConductorOptions options) {
            throw new UnsupportedOperationException();
          }

          @Override
          public SuspendBackend createSuspendBackend(EnterpriseEngine engine) {
            throw new UnsupportedOperationException();
          }
        };
    var e =
        assertThrows(
            IllegalStateException.class, () -> provider.createAutoUpgradePass(null, Duration.ZERO));
    assertTrue(e.getMessage().startsWith("Auto-upgrading workflows requires"), e.getMessage());
  }

  @Test
  public void anEnterpriseBuildTooOldToLoadIsNamedAsTheWrongVersion() {
    var e =
        assertThrows(
            IllegalStateException.class,
            () ->
                EnterpriseProviders.select(
                    List.of(), "1.4.0", EnterpriseProviders.CONDUCTOR, true));
    assertEquals(
        "Connecting to DBOS Conductor requires dev.dbos:transact-enterprise built for this"
            + " dev.dbos:transact, but the one on the class path is a 1.3 or earlier build."
            + " Replace it with dev.dbos:transact-enterprise:1.4.0, the same version as"
            + " dev.dbos:transact",
        e.getMessage());
  }

  @Test
  public void findsTheRegistrationOfAnEnterpriseBuildTooOldToLoad(@TempDir Path dir)
      throws Exception {
    var registration = dir.resolve(EnterpriseProviders.LEGACY_REGISTRATION);
    Files.createDirectories(registration.getParent());
    Files.writeString(registration, "dev.dbos.transact.enterprise.EnterpriseConductorProvider\n");

    try (var legacy = new URLClassLoader(new URL[] {dir.toUri().toURL()}, null);
        var empty = new URLClassLoader(new URL[0], null)) {
      assertTrue(EnterpriseProviders.hasLegacyRegistration(List.of(empty, legacy)));
      assertFalse(EnterpriseProviders.hasLegacyRegistration(List.of(empty)));
    }
  }
}
