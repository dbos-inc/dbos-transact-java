package dev.dbos.transact.internal;

import dev.dbos.transact.DBOS;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.regex.Pattern;

/**
 * Finds the implementation of a Transact Enterprise provider interface on the class path. Core
 * declares each interface; the {@code dev.dbos:transact-enterprise} artifact registers its
 * implementations with {@link ServiceLoader}. A feature that needs a license is loaded only once
 * {@link Entitlements#require} has passed for it.
 */
public final class EnterpriseProviders {
  public static final String ENTERPRISE_ARTIFACT = "dev.dbos:transact-enterprise";

  private static final Pattern RELEASE = Pattern.compile("\\d+\\.\\d+\\.\\d+");
  private static final Pattern MINOR = Pattern.compile("(\\d+\\.\\d+)\\..*");

  private EnterpriseProviders() {}

  /**
   * Returns the one registered implementation of {@code type}. Looks in the thread context class
   * loader and in the one that loaded DBOS, so a provider deployed beside the application is found
   * when DBOS itself is a shared library, as in a servlet container.
   *
   * @param type the provider interface
   * @param feature what needs the provider, as errors name it (e.g. "Connecting to DBOS Conductor")
   * @throws IllegalStateException if none is registered, more than one is, or one fails to
   *     initialize
   */
  public static <T> T load(Class<T> type, String feature) {
    var loaders = new ArrayList<ClassLoader>();
    var context = Thread.currentThread().getContextClassLoader();
    if (context != null) {
      loaders.add(context);
    }
    var own = type.getClassLoader();
    if (own != context) {
      loaders.add(own);
    }

    // One instance per provider class, constructed once: a loader that delegates to its parent sees
    // the parent's registrations too, so duplicates are dropped before they are instantiated.
    var providers = new LinkedHashMap<Class<?>, T>();
    try {
      for (var loader : loaders) {
        ServiceLoader.load(type, loader).stream()
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
    return select(type, List.copyOf(providers.values()), DBOS.version(), feature);
  }

  /** The one provider in {@code providers}, or an error naming the artifact to add. */
  public static <T> T select(Class<T> type, List<T> providers, String version, String feature) {
    if (providers.isEmpty()) {
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
          "Found more than one " + type.getSimpleName() + " on the class path: " + names);
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
