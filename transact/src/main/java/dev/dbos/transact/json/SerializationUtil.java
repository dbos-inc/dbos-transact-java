package dev.dbos.transact.json;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Utility class for serialization and deserialization with support for multiple formats.
 *
 * <p>This class handles the logic of choosing the appropriate serializer based on the serialization
 * format stored in the database. It supports:
 *
 * <ul>
 *   <li>{@code portable_json} - Portable format compatible with any language
 *   <li>{@code java_serialize} - Native Java format with type information
 *   <li>Custom serializers registered by the application
 * </ul>
 */
public final class SerializationUtil {

  /** Serialization format for portable JSON (cross-language compatible). */
  public static final String PORTABLE = DBOSPortableSerializer.NAME;

  /** Serialization format for native Java serialization. */
  public static final String NATIVE = DBOSJavaSerializer.NAME;

  private SerializationUtil() {}

  // ============ Value Serialization ============

  /**
   * Serialize a value using the specified format.
   *
   * @param value the value to serialize
   * @param format the serialization format ("portable_json", "java_jackson", or a custom serializer
   *     name)
   * @param customSerializer optional custom serializer (used if format is not portable/native)
   * @return the serialized result containing the serialized string and the serializer name
   */
  public static SerializedResult serializeValue(
      Object value, String format, DBOSSerializer customSerializer) {

    if (PORTABLE.equals(format)) {
      String serialized = DBOSPortableSerializer.INSTANCE.stringify(value);
      return new SerializedResult(serialized, DBOSPortableSerializer.NAME);
    }

    if (NATIVE.equals(format)) {
      String serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
      return new SerializedResult(serialized, DBOSJavaSerializer.NAME);
    }

    if (format == null) {
      // Default behavior: use native serializer but don't store format (backward compatibility)
      String serialized = DBOSJavaSerializer.INSTANCE.stringify(value);
      return new SerializedResult(serialized, null);
    }

    // Custom serializer
    DBOSSerializer serializer =
        customSerializer != null ? customSerializer : DBOSJavaSerializer.INSTANCE;
    String serialized = serializer.stringify(value);
    return new SerializedResult(serialized, serializer.name());
  }

  /**
   * Deserialize a value using the serialization format stored with it.
   *
   * @param serializedValue the serialized string
   * @param serialization the serialization format name (from DB column)
   * @param customSerializer optional custom serializer
   * @return the deserialized value
   */
  public static Object deserializeValue(
      String serializedValue, String serialization, DBOSSerializer customSerializer) {

    if (serializedValue == null) {
      return null;
    }

    if (DBOSPortableSerializer.NAME.equals(serialization)) {
      return DBOSPortableSerializer.INSTANCE.parse(serializedValue);
    }

    if (DBOSJavaSerializer.NAME.equals(serialization) || serialization == null) {
      return DBOSJavaSerializer.INSTANCE.parse(serializedValue);
    }

    // Try custom serializer
    if (customSerializer != null && customSerializer.name().equals(serialization)) {
      return customSerializer.parse(serializedValue);
    }

    // Fallback to native Java serializer for unknown formats
    return DBOSJavaSerializer.INSTANCE.parse(serializedValue);
  }

  // ============ Arguments Serialization ============

  /**
   * Serialize workflow arguments using the specified format.
   *
   * @param positionalArgs the positional arguments
   * @param namedArgs the named arguments (only supported for portable format)
   * @param format the serialization format
   * @param customSerializer optional custom serializer
   * @return the serialized result
   */
  public static SerializedResult serializeArgs(
      Object[] positionalArgs,
      Map<String, Object> namedArgs,
      String format,
      DBOSSerializer customSerializer) {

    if (PORTABLE.equals(format)) {
      String serialized = DBOSPortableSerializer.INSTANCE.stringifyArgs(positionalArgs, namedArgs);
      return new SerializedResult(serialized, DBOSPortableSerializer.NAME);
    }

    if (namedArgs != null && !namedArgs.isEmpty()) {
      throw new IllegalArgumentException(
          "Serialization format '" + format + "' does not support named arguments");
    }

    if (NATIVE.equals(format) || format == null) {
      String serialized = DBOSJavaSerializer.INSTANCE.stringifyArray(positionalArgs);
      return new SerializedResult(serialized, DBOSJavaSerializer.NAME);
    }

    // Custom serializer
    DBOSSerializer serializer =
        customSerializer != null ? customSerializer : DBOSJavaSerializer.INSTANCE;
    String serialized = serializer.stringify(positionalArgs);
    return new SerializedResult(serialized, serializer.name());
  }

  /**
   * Deserialize workflow arguments (positional only).
   *
   * @param serializedValue the serialized string
   * @param serialization the serialization format name
   * @param customSerializer optional custom serializer
   * @return the positional arguments array
   */
  public static Object[] deserializePositionalArgs(
      String serializedValue, String serialization, DBOSSerializer customSerializer) {

    if (serializedValue == null) {
      return new Object[0];
    }

    if (DBOSPortableSerializer.NAME.equals(serialization)) {
      JsonWorkflowArgs args = DBOSPortableSerializer.INSTANCE.parseArgs(serializedValue);
      if (args == null || args.positionalArgs() == null) {
        return new Object[0];
      }
      return args.positionalArgs().toArray();
    }

    if (DBOSJavaSerializer.NAME.equals(serialization) || serialization == null) {
      return DBOSJavaSerializer.INSTANCE.parseArray(serializedValue);
    }

    // Try custom serializer
    if (customSerializer != null && customSerializer.name().equals(serialization)) {
      Object result = customSerializer.parse(serializedValue);
      if (result instanceof Object[]) {
        return (Object[]) result;
      }
      if (result instanceof List<?> list) {
        return list.toArray();
      }
      return new Object[] {result};
    }

    // Fallback
    return DBOSJavaSerializer.INSTANCE.parseArray(serializedValue);
  }

  // ============ Error Serialization ============

  /**
   * Serialize an error using the specified format.
   *
   * @param error the error to serialize
   * @param format the serialization format
   * @param customSerializer optional custom serializer
   * @return the serialized result
   */
  public static SerializedResult serializeError(
      Throwable error, String format, DBOSSerializer customSerializer) {

    if (PORTABLE.equals(format)) {
      String serialized = DBOSPortableSerializer.INSTANCE.stringifyError(error);
      return new SerializedResult(serialized, DBOSPortableSerializer.NAME);
    }

    if (NATIVE.equals(format) || format == null) {
      // Use the existing Java error serialization
      String serialized = JSONUtil.serializeAppException(error);
      return new SerializedResult(serialized, DBOSJavaSerializer.NAME);
    }

    // Custom serializer - use native Java format
    String serialized = JSONUtil.serializeAppException(error);
    DBOSSerializer serializer =
        customSerializer != null ? customSerializer : DBOSJavaSerializer.INSTANCE;
    return new SerializedResult(serialized, serializer.name());
  }

  /**
   * Deserialize an error.
   *
   * @param serializedValue the serialized string
   * @param serialization the serialization format name
   * @param customSerializer optional custom serializer
   * @return the deserialized throwable
   */
  public static Throwable deserializeError(
      String serializedValue, String serialization, DBOSSerializer customSerializer) {

    if (serializedValue == null) {
      return null;
    }

    if (DBOSPortableSerializer.NAME.equals(serialization)) {
      return DBOSPortableSerializer.INSTANCE.parseError(serializedValue);
    }

    if (DBOSJavaSerializer.NAME.equals(serialization) || serialization == null) {
      return JSONUtil.deserializeAppException(serializedValue);
    }

    // Try custom or fall back to Java
    try {
      return JSONUtil.deserializeAppException(serializedValue);
    } catch (Exception e) {
      // Return a generic exception with the message
      return new RuntimeException("Deserialization failed for format: " + serialization, e);
    }
  }

  /**
   * Safely parse a value, returning the raw string if parsing fails. Used for introspection methods
   * that may encounter old or undeserializable data.
   */
  public static Object safeParse(
      String serializedValue, String serialization, DBOSSerializer customSerializer) {
    try {
      return deserializeValue(serializedValue, serialization, customSerializer);
    } catch (Exception e) {
      return serializedValue;
    }
  }

  /** Safely parse arguments, returning the raw string if parsing fails. */
  public static Object safeParseArgs(
      String serializedValue, String serialization, DBOSSerializer customSerializer) {
    try {
      return deserializePositionalArgs(serializedValue, serialization, customSerializer);
    } catch (Exception e) {
      return serializedValue;
    }
  }

  /** Safely parse an error, returning a RuntimeException with the raw message if parsing fails. */
  public static Throwable safeParseError(
      String serializedValue, String serialization, DBOSSerializer customSerializer) {
    try {
      return deserializeError(serializedValue, serialization, customSerializer);
    } catch (Exception e) {
      return new RuntimeException(serializedValue);
    }
  }

  /**
   * Result of a serialization operation, containing both the serialized string and the name of the
   * serializer used (to be stored in the DB).
   */
  /** Result of serialization, containing the serialized string and the format used. */
  public record SerializedResult(String serializedValue, String serialization) {
    public SerializedResult {
      Objects.requireNonNull(serializedValue);
      // serialization can be null for backward compatibility (default format)
    }
  }
}
