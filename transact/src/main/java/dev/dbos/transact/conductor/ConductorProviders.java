package dev.dbos.transact.conductor;

import dev.dbos.transact.DBOS;

import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/** Finds the {@link ConductorProvider} on the class path. */
public final class ConductorProviders {
  static final String ENTERPRISE_ARTIFACT = "dev.dbos:transact-enterprise";

  private ConductorProviders() {}

  /**
   * Returns the one registered {@link ConductorProvider}.
   *
   * @throws IllegalStateException if none is registered, more than one is, or one fails to
   *     initialize
   */
  public static ConductorProvider load() {
    List<ConductorProvider> providers;
    try {
      providers =
          ServiceLoader.load(ConductorProvider.class, ConductorProvider.class.getClassLoader())
              .stream()
              .map(ServiceLoader.Provider::get)
              .toList();
    } catch (ServiceConfigurationError e) {
      // A provider's constructor rejected this DBOS, e.g. for a version mismatch. Its own message
      // says why; ServiceLoader's wrapper only names the class.
      var cause = e.getCause();
      throw new IllegalStateException(cause != null ? cause.getMessage() : e.getMessage(), e);
    }
    return select(providers);
  }

  static ConductorProvider select(List<ConductorProvider> providers) {
    if (providers.isEmpty()) {
      var version = DBOS.version();
      throw new IllegalStateException(
          String.format(
              "Connecting to DBOS Conductor requires %s at the same version as dev.dbos:transact"
                  + " (%s). Add implementation(\"%s:%s\") to your dependencies",
              ENTERPRISE_ARTIFACT, version, ENTERPRISE_ARTIFACT, version));
    }
    if (providers.size() > 1) {
      var names = providers.stream().map(p -> p.getClass().getName()).toList();
      throw new IllegalStateException(
          "Found more than one DBOS Conductor provider on the class path: " + names);
    }
    return providers.get(0);
  }
}
