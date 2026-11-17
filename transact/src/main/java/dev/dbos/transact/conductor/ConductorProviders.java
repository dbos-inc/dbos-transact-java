package dev.dbos.transact.conductor;

import dev.dbos.transact.internal.EnterpriseProviders;

import java.util.List;

/** Finds the {@link ConductorProvider} on the class path. */
public final class ConductorProviders {
  /** What needs the provider when DBOS connects to Conductor, which needs no entitlement. */
  public static final String CONDUCTOR = "Connecting to DBOS Conductor";

  private ConductorProviders() {}

  /**
   * Returns the one registered {@link ConductorProvider}. Looks in the thread context class loader
   * and in the one that loaded DBOS, so a provider deployed beside the application is found when
   * DBOS itself is a shared library, as in a servlet container.
   *
   * @param feature what needs the provider, as errors name it
   * @throws IllegalStateException if none is registered, more than one is, or one fails to
   *     initialize
   */
  public static ConductorProvider load(String feature) {
    return EnterpriseProviders.load(ConductorProvider.class, feature);
  }

  static ConductorProvider select(
      List<ConductorProvider> providers, String version, String feature) {
    return EnterpriseProviders.select(ConductorProvider.class, providers, version, feature);
  }
}
