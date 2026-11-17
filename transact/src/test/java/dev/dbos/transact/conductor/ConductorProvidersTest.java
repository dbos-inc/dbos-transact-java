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
  public void loadsTheRegisteredProvider() {
    assertInstanceOf(
        TestConductorProvider.class, ConductorProviders.load(ConductorProviders.CONDUCTOR));
  }

  @Test
  public void aContextLoaderThatDelegatesToDbosFindsItOnce() throws Exception {
    // The child sees the test registration through its parent; finding it twice would read as
    // two providers.
    var thread = Thread.currentThread();
    var previous = thread.getContextClassLoader();
    try (var child = new URLClassLoader(new URL[0], ConductorProvider.class.getClassLoader())) {
      thread.setContextClassLoader(child);
      assertInstanceOf(
          TestConductorProvider.class, ConductorProviders.load(ConductorProviders.CONDUCTOR));
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  @Test
  public void selectsTheOnlyProvider() {
    var provider = new TestConductorProvider();
    assertSame(
        provider,
        ConductorProviders.select(List.of(provider), "1.3.0", ConductorProviders.CONDUCTOR));
  }

  @Test
  public void noProviderNamesTheReleaseToAdd() {
    var e =
        assertThrows(
            IllegalStateException.class,
            () -> ConductorProviders.select(List.of(), "1.3.0", ConductorProviders.CONDUCTOR));
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
            () -> ConductorProviders.select(List.of(), "1.3.0-m25", ConductorProviders.CONDUCTOR));
    assertEquals(
        "Connecting to DBOS Conductor requires dev.dbos:transact-enterprise on the class path."
            + " Add a dev.dbos:transact-enterprise 1.3 build, as dev.dbos:transact is 1.3.0-m25",
        e.getMessage());
  }

  @Test
  public void moreThanOneProviderIsRejected() {
    var providers =
        List.<ConductorProvider>of(new TestConductorProvider(), new TestConductorProvider());
    assertThrows(
        IllegalStateException.class,
        () -> ConductorProviders.select(providers, "1.3.0", ConductorProviders.CONDUCTOR));
  }

  @Test
  public void noProviderNamesTheFeatureThatNeedsIt() {
    var e =
        assertThrows(
            IllegalStateException.class,
            () ->
                ConductorProviders.select(
                    List.of(), "1.4.0", "Suspending workflows on recv and sleep"));
    assertEquals(
        "Suspending workflows on recv and sleep requires dev.dbos:transact-enterprise on the class"
            + " path. Add dev.dbos:transact-enterprise:1.4.0, the same version as"
            + " dev.dbos:transact",
        e.getMessage());
  }
}
