package dev.dbos.transact.conductor;

import dev.dbos.transact.internal.EnterpriseProviders;

import java.util.List;

/** Finds the {@link ConductorProvider} on the class path. */
public final class ConductorProviders {
  // Connecting to Conductor needs the enterprise artifact, but no entitlement.
  private static final String FEATURE = "Connecting to DBOS Conductor";

  private ConductorProviders() {}

  /**
   * Returns the one registered {@link ConductorProvider}. Looks in the thread context class loader
   * and in the one that loaded DBOS, so a provider deployed beside the application is found when
   * DBOS itself is a shared library, as in a servlet container.
   *
   * @throws IllegalStateException if none is registered, more than one is, or one fails to
   *     initialize
   */
  public static ConductorProvider load() {
    return EnterpriseProviders.load(ConductorProvider.class, FEATURE);
  }

  static ConductorProvider select(List<ConductorProvider> providers, String version) {
    return EnterpriseProviders.select(ConductorProvider.class, providers, version, FEATURE);
  }
}
