package dev.dbos.transact.json;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

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

  private final ObjectMapper mapper;

  public DBOSPortableSerializer() {
    this.mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    // Write dates as ISO-8601 strings for portability
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String stringify(Object value, boolean _noHistoricalWrapper) {
    try {
      return mapper.writeValueAsString(toPortable(value));
    } catch (JsonProcessingException e) {
      throw new JSONUtil.JsonRuntimeException(e);
    }
  }

  @Override
  public Object parse(String text, boolean _noHistoricalWrapper) {
    if (text == null) {
      return null;
    }
    try {
      return mapper.readValue(text, Object.class);
    } catch (JsonProcessingException e) {
      throw new JSONUtil.JsonRuntimeException(e);
    }
  }

  /** Serialize workflow arguments in portable format. */
  public String stringifyArgs(Object[] positionalArgs, Map<String, Object> namedArgs) {
    JsonWorkflowArgs args =
        new JsonWorkflowArgs(positionalArgs, namedArgs != null ? toPortableMap(namedArgs) : null);
    try {
      return mapper.writeValueAsString(args);
    } catch (JsonProcessingException e) {
      throw new JSONUtil.JsonRuntimeException(e);
    }
  }

  /** Deserialize workflow arguments from portable format. */
  public JsonWorkflowArgs parseArgs(String text) {
    if (text == null) {
      return null;
    }
    try {
      return mapper.readValue(text, JsonWorkflowArgs.class);
    } catch (JsonProcessingException e) {
      throw new JSONUtil.JsonRuntimeException(e);
    }
  }

  /** Serialize an error in portable format. */
  public String stringifyError(Throwable error) {
    JsonWorkflowErrorData errorData =
        new JsonWorkflowErrorData(
            error.getClass().getSimpleName(),
            error.getMessage(),
            error instanceof PortableWorkflowException pwe ? pwe.getCode() : null,
            error instanceof PortableWorkflowException pwe ? pwe.getData() : null);
    try {
      return mapper.writeValueAsString(errorData);
    } catch (JsonProcessingException e) {
      throw new JSONUtil.JsonRuntimeException(e);
    }
  }

  /** Deserialize an error from portable format. */
  public PortableWorkflowException parseError(String text) {
    if (text == null) {
      return null;
    }
    try {
      JsonWorkflowErrorData errorData = mapper.readValue(text, JsonWorkflowErrorData.class);
      return PortableWorkflowException.fromErrorData(errorData);
    } catch (JsonProcessingException e) {
      throw new JSONUtil.JsonRuntimeException(e);
    }
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

  @SuppressWarnings("unchecked")
  private Map<String, Object> toPortableMap(Map<String, Object> map) {
    java.util.HashMap<String, Object> result = new java.util.HashMap<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      result.put(entry.getKey(), toPortable(entry.getValue()));
    }
    return result;
  }
}
