package dev.dbos.transact.internal;

import dev.dbos.transact.json.JsonUtility;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

public class Validation {

  /**
   * An application name is durable, cross-language identity: it is written onto every row the
   * application owns, and peers in other SDKs read it back. So a name must be one every SDK could
   * hold. Python's rule, _is_valid_app_name (_dbos_config.py:562), is the one they all enforce.
   */
  private static final java.util.regex.Pattern APP_NAME_PATTERN =
      java.util.regex.Pattern.compile("^[a-z0-9-_]{3,30}$");

  /** Whether a name can be an application's durable identity. See {@link #APP_NAME_PATTERN}. */
  public static boolean isValidApplicationName(@Nullable String name) {
    return name != null && APP_NAME_PATTERN.matcher(name).matches();
  }

  /** The message every rejection of an application name uses, so operators see one rule. */
  public static String invalidApplicationName(String what, @Nullable String name) {
    return ("Invalid %s '%s'. Application names must be between 3 and 30 characters long and"
            + " contain only lowercase letters, numbers, dashes, and underscores.")
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
