package dev.dbos.transact.internal;

import dev.dbos.transact.json.JsonUtility;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

public class Validation {

  /**
   * The application names DBOS Conductor and DBOS Cloud accept at registration (their
   * isValidApplicationName validator). Nothing in Transact needs a name to look like this: the
   * column is TEXT, the value is always a bound parameter, and the version hash just digests the
   * bytes. Go, TypeScript, and dbosctl do not check it at all. So this classifies a name rather
   * than gating one -- an application that never registers with Conductor can be called anything.
   */
  private static final java.util.regex.Pattern APP_NAME_PATTERN =
      java.util.regex.Pattern.compile("^[a-z0-9-_]{3,30}$");

  /** Whether Conductor and Cloud would accept {@code name}. See {@link #APP_NAME_PATTERN}. */
  public static boolean isValidApplicationName(@Nullable String name) {
    return name != null && APP_NAME_PATTERN.matcher(name).matches();
  }

  /** The message every warning about an application name uses, so operators see one rule. */
  public static String applicationNameNotAcceptedByConductor(String what, @Nullable String name) {
    return ("The %s '%s' cannot be registered with DBOS Conductor or DBOS Cloud, which accept"
            + " between 3 and 30 characters, and only lowercase letters, numbers, dashes, and"
            + " underscores.")
        .formatted(what, name);
  }

  public static boolean nullableIsEmpty(@Nullable String value) {
    return value != null && value.isEmpty();
  }

  public static boolean nullableIsNotPositive(@Nullable Duration value) {
    return value != null && (value.isNegative() || value.isZero());
  }

  /**
   * Validate that the supplied workflow attributes are JSON-serializable and return an unmodifiable
   * defensive copy (or {@code null}). Fails fast here rather than surfacing an opaque error later
   * when the workflow status is recorded as JSON. {@code null} values within the map are permitted.
   */
  public static @Nullable Map<String, Object> validateAttributes(
      @Nullable Map<String, Object> attributes) {
    if (attributes == null) {
      return null;
    }
    try {
      JsonUtility.toJson(attributes);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "Invalid workflow attributes " + attributes + ". Attributes must be JSON-serializable.",
          e);
    }
    // LinkedHashMap (not Map.copyOf) so null values are permitted.
    return java.util.Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
  }
}
