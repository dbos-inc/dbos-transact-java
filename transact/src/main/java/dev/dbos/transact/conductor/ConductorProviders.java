package dev.dbos.transact.conductor;

import dev.dbos.transact.DBOS;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.regex.Pattern;

/** Finds the {@link ConductorProvider} on the class path. */
public final class ConductorProviders {
  static final String ENTERPRISE_ARTIFACT = "dev.dbos:transact-enterprise";

  private static final Pattern RELEASE = Pattern.compile("\\d+\\.\\d+\\.\\d+");
  private static final Pattern MINOR = Pattern.compile("(\\d+\\.\\d+)\\..*");

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
    var loaders = new ArrayList<ClassLoader>();
    var context = Thread.currentThread().getContextClassLoader();
    if (context != null) {
      loaders.add(context);
    }
    var own = ConductorProvider.class.getClassLoader();
    if (own != context) {
      loaders.add(own);
    }

    // One instance per provider class: a loader that delegates to its parent sees the parent's
    // registrations too.
    var providers = new LinkedHashMap<Class<?>, ConductorProvider>();
    try {
      for (var loader : loaders) {
        for (var provider : ServiceLoader.load(ConductorProvider.class, loader)) {
          providers.putIfAbsent(provider.getClass(), provider);
        }
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
    return select(List.copyOf(providers.values()), DBOS.version());
  }

  static ConductorProvider select(List<ConductorProvider> providers, String version) {
    if (providers.isEmpty()) {
      throw new IllegalStateException(
          "Connecting to DBOS Conductor requires "
              + ENTERPRISE_ARTIFACT
              + " on the class path. "
              + versionAdvice(version));
    }
    if (providers.size() > 1) {
      var names = providers.stream().map(p -> p.getClass().getName()).toList();
      throw new IllegalStateException(
          "Found more than one DBOS Conductor provider on the class path: " + names);
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
