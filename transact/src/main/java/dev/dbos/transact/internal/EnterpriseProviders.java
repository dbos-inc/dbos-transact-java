package dev.dbos.transact.internal;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.spi.EnterpriseProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.regex.Pattern;

/**
 * Finds the {@link EnterpriseProvider} on the class path: the one entry point through which the
 * {@code dev.dbos:transact-enterprise} artifact provides every enterprise feature, registered with
 * {@link ServiceLoader}. A feature that needs a license is loaded only once {@link
 * Entitlements#require} has passed for it.
 */
public final class EnterpriseProviders {
  public static final String ENTERPRISE_ARTIFACT = "dev.dbos:transact-enterprise";

  /** What needs the provider when DBOS connects to Conductor, which needs no entitlement. */
  public static final String CONDUCTOR = "Connecting to DBOS Conductor";

  /**
   * Where a {@code dev.dbos:transact-enterprise} 1.3 or earlier registered its provider, under the
   * interface's old name. Such a build is never loaded, but finding its registration lets the error
   * say the artifact is there and the wrong version, rather than missing.
   */
  static final String LEGACY_REGISTRATION =
      "META-INF/services/dev.dbos.transact.conductor.ConductorProvider";

  private static final Pattern RELEASE = Pattern.compile("\\d+\\.\\d+\\.\\d+");
  private static final Pattern MINOR = Pattern.compile("(\\d+\\.\\d+)\\..*");

  private EnterpriseProviders() {}

  /**
   * Returns the one registered {@link EnterpriseProvider}. Looks in the thread context class loader
   * and in the one that loaded DBOS, so a provider deployed beside the application is found when
   * DBOS itself is a shared library, as in a servlet container.
   *
   * @param feature what needs the provider, as errors name it (e.g. {@link #CONDUCTOR})
   * @throws IllegalStateException if none is registered, more than one is, or one fails to
   *     initialize
   */
  public static EnterpriseProvider load(String feature) {
    var loaders = new ArrayList<ClassLoader>();
    var context = Thread.currentThread().getContextClassLoader();
    if (context != null) {
      loaders.add(context);
    }
    var own = EnterpriseProvider.class.getClassLoader();
    if (own != context) {
      loaders.add(own);
    }

    // One instance per provider class, constructed once: a loader that delegates to its parent sees
    // the parent's registrations too, so duplicates are dropped before they are instantiated.
    var providers = new LinkedHashMap<Class<?>, EnterpriseProvider>();
    try {
      for (var loader : loaders) {
        ServiceLoader.load(EnterpriseProvider.class, loader).stream()
            .filter(provider -> !providers.containsKey(provider.type()))
            .forEach(provider -> providers.put(provider.type(), provider.get()));
      }
    } catch (ServiceConfigurationError e) {
      // A provider's constructor rejected this DBOS, e.g. for a version mismatch. Its own message
      // says why; ServiceLoader's names the provider.
      var cause = e.getCause();
      var message =
          cause != null && cause.getMessage() != null
              ? e.getMessage() + ": " + cause.getMessage()
              : e.getMessage();
      throw new IllegalStateException(message, e);
    }
    var found = List.copyOf(providers.values());
    var legacy = found.isEmpty() && hasLegacyRegistration(loaders);
    return select(found, DBOS.version(), feature, legacy);
  }

  /** Whether any of {@code loaders} sees a 1.3-or-earlier enterprise registration. */
  static boolean hasLegacyRegistration(List<ClassLoader> loaders) {
    for (var loader : loaders) {
      try {
        if (loader.getResources(LEGACY_REGISTRATION).hasMoreElements()) {
          return true;
        }
      } catch (IOException e) {
        // Only sharpens the error message; an unreadable class path reads as no registration.
      }
    }
    return false;
  }

  /**
   * The one provider in {@code providers}, or an error naming the artifact to add.
   *
   * @param legacy whether an enterprise build too old to load is on the class path
   */
  static EnterpriseProvider select(
      List<EnterpriseProvider> providers, String version, String feature, boolean legacy) {
    if (providers.isEmpty()) {
      if (legacy) {
        throw new IllegalStateException(
            feature
                + " requires "
                + ENTERPRISE_ARTIFACT
                + " of the same minor version as dev.dbos:transact, but the one on the class path"
                + " is a 1.3 or earlier build. "
                + versionAdvice(version));
      }
      throw new IllegalStateException(
          feature
              + " requires "
              + ENTERPRISE_ARTIFACT
              + " on the class path. "
              + versionAdvice(version));
    }
    if (providers.size() > 1) {
      var names = providers.stream().map(p -> p.getClass().getName()).toList();
      throw new IllegalStateException(
          "Found more than one "
              + EnterpriseProvider.class.getSimpleName()
              + " on the class path: "
              + names);
    }
    return providers.get(0);
  }

  // Previews are numbered by commits in each repo, so only a release names the exact version.
  private static String versionAdvice(String version) {
    if (version != null && RELEASE.matcher(version).matches()) {
      return String.format(
          "Add %s:%s, the same version as dev.dbos:transact", ENTERPRISE_ARTIFACT, version);
    }
    var minor = version == null ? null : MINOR.matcher(version);
    if (minor != null && minor.matches()) {
      return String.format(
          "Add a %s %s build, as dev.dbos:transact is %s",
          ENTERPRISE_ARTIFACT, minor.group(1), version);
    }
    return "Add the " + ENTERPRISE_ARTIFACT + " version that matches dev.dbos:transact";
  }
}
