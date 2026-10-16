package dev.dbos.transact.conductor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.junit.jupiter.api.Test;

public class ConductorProvidersTest {

  @Test
  public void loadsTheBundledProvider() {
    assertInstanceOf(BundledConductorProvider.class, ConductorProviders.load());
  }

  @Test
  public void aContextLoaderThatDelegatesToDbosFindsItOnce() throws Exception {
    // The child sees the bundled registration through its parent; finding it twice would read as
    // two providers.
    var thread = Thread.currentThread();
    var previous = thread.getContextClassLoader();
    try (var child = new URLClassLoader(new URL[0], ConductorProvider.class.getClassLoader())) {
      thread.setContextClassLoader(child);
      assertInstanceOf(BundledConductorProvider.class, ConductorProviders.load());
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  @Test
  public void selectsTheOnlyProvider() {
    var provider = new BundledConductorProvider();
    assertSame(provider, ConductorProviders.select(List.of(provider), "1.3.0"));
  }

  @Test
  public void noProviderNamesTheReleaseToAdd() {
    var e =
        assertThrows(
            IllegalStateException.class, () -> ConductorProviders.select(List.of(), "1.3.0"));
    assertEquals(
        "Connecting to DBOS Conductor requires dev.dbos:transact-enterprise on the class path."
            + " Add dev.dbos:transact-enterprise:1.3.0, the same version as dev.dbos:transact",
        e.getMessage());
  }

  @Test
  public void noProviderOnAPreviewNamesTheMinor() {
    var e =
        assertThrows(
            IllegalStateException.class, () -> ConductorProviders.select(List.of(), "1.3.0-m25"));
    assertEquals(
        "Connecting to DBOS Conductor requires dev.dbos:transact-enterprise on the class path."
            + " Add a dev.dbos:transact-enterprise 1.3 build, as dev.dbos:transact is 1.3.0-m25",
        e.getMessage());
  }

  @Test
  public void moreThanOneProviderIsRejected() {
    var providers =
        List.<ConductorProvider>of(new BundledConductorProvider(), new BundledConductorProvider());
    assertThrows(IllegalStateException.class, () -> ConductorProviders.select(providers, "1.3.0"));
  }
}
