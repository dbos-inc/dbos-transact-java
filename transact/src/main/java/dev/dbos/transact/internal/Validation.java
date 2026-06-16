package dev.dbos.transact.internal;

import dev.dbos.transact.json.JsonUtility;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

public class Validation {

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
