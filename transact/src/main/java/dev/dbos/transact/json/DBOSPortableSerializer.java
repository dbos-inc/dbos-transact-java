package dev.dbos.transact.json;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Portable JSON serializer that produces output compatible with any language. Does not include
 * Java-specific type information.
 *
 * <p>Dates are serialized as ISO-8601 strings. Maps and Sets are serialized as plain JSON
 * objects/arrays. Does not preserve Java class information.
 */
public class DBOSPortableSerializer implements DBOSSerializer {

  public static final String NAME = "portable_json";

  public static final DBOSPortableSerializer INSTANCE = new DBOSPortableSerializer();

  public DBOSPortableSerializer() {}

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String stringify(Object value) {
    return JsonUtility.toJson(toPortable(value));
  }

  @Override
  public Object parse(String text) {
    if (text == null) {
      return null;
    }
    return JsonUtility.fromJson(text, Object.class);
  }

  /** Serialize workflow arguments in portable format. */
  public String stringifyArgs(Object[] positionalArgs, Map<String, Object> namedArgs) {
    JsonWorkflowArgs args =
        new JsonWorkflowArgs(positionalArgs, namedArgs != null ? toPortableMap(namedArgs) : null);
    return JsonUtility.toJson(args);
  }

  /** Deserialize workflow arguments from portable format. */
  public JsonWorkflowArgs parseArgs(String text) {
    if (text == null) {
      return null;
    }
    return JsonUtility.fromJson(text, JsonWorkflowArgs.class);
  }

  /** Serialize an error in portable format. */
  public String stringifyThrowable(Throwable error) {
    String name;
    Object code = null;
    Object data = null;
    if (error instanceof PortableWorkflowException pwe) {
      name = pwe.getErrorName();
      code = pwe.getCode();
      data = pwe.getData();
    } else {
      name = error.getClass().getSimpleName();
    }
    return JsonUtility.toJson(new JsonWorkflowErrorData(name, error.getMessage(), code, data));
  }

  /** Deserialize an error from portable format. */
  public Throwable parseThrowable(String text) {
    if (text == null) {
      return null;
    }
    JsonWorkflowErrorData errorData = JsonUtility.fromJson(text, JsonWorkflowErrorData.class);
    return PortableWorkflowException.fromErrorData(errorData);
  }

  /**
   * Convert a value to its portable representation. - Dates become ISO-8601 strings - Other objects
   * pass through (Jackson handles them)
   */
  @SuppressWarnings("unchecked")
  private Object toPortable(Object value) {
    if (value == null) {
      return null;
    }

    // Convert dates to ISO-8601 strings
    if (value instanceof Date date) {
      return DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
    }
    if (value instanceof Instant instant) {
      return DateTimeFormatter.ISO_INSTANT.format(instant);
    }
    if (value instanceof OffsetDateTime odt) {
      return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(odt);
    }
    if (value instanceof ZonedDateTime zdt) {
      return DateTimeFormatter.ISO_ZONED_DATE_TIME.format(zdt);
    }

    // Convert arrays recursively
    if (value instanceof Object[] array) {
      return toPortableArray(array);
    }

    // Convert lists recursively
    if (value instanceof List<?> list) {
      return list.stream().map(this::toPortable).toList();
    }

    // Convert maps recursively
    if (value instanceof Map<?, ?> map) {
      return toPortableMap((Map<String, Object>) map);
    }

    // Errors become error data
    if (value instanceof Throwable t) {
      return new JsonWorkflowErrorData(t.getClass().getSimpleName(), t.getMessage());
    }

    return value;
  }

  private Object[] toPortableArray(Object[] array) {
    Object[] result = new Object[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = toPortable(array[i]);
    }
    return result;
  }

  private Map<String, Object> toPortableMap(Map<String, Object> map) {
    java.util.HashMap<String, Object> result = new java.util.HashMap<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      result.put(entry.getKey(), toPortable(entry.getValue()));
    }
    return result;
  }
}
