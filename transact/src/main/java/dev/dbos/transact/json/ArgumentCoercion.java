package dev.dbos.transact.json;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Utility for coercing deserialized arguments to match the expected parameter types of a workflow
 * method. Uses Jackson's {@link ObjectMapper#convertValue} for type conversion.
 *
 * <p>When JSON is deserialized (especially portable JSON), values become generic Java types
 * (Integer, Double, ArrayList, LinkedHashMap, String) without the specific type information that
 * the Java method expects. This class bridges that gap by using reflection to determine expected
 * types and Jackson to perform the conversion. This includes converting ISO-8601 date strings to
 * Java temporal types (Instant, OffsetDateTime, etc.).
 */
public final class ArgumentCoercion {

  private static final ObjectMapper mapper = createMapper();

  private static ObjectMapper createMapper() {
    ObjectMapper m = new ObjectMapper();
    m.registerModule(new JavaTimeModule());
    m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    return m;
  }

  private ArgumentCoercion() {}

  /**
   * Coerce deserialized arguments to match the expected parameter types of a method.
   *
   * @param args the deserialized arguments (from portable JSON)
   * @param method the workflow method whose parameter types define the target types
   * @return a new array with each argument converted to the expected type
   * @throws IllegalArgumentException if the argument count doesn't match or a conversion is
   *     impossible
   */
  public static Object[] coerceArguments(Object[] args, Method method) {
    int expected = method.getParameterCount();
    if (args.length != expected) {
      throw new IllegalArgumentException(
          "Expected "
              + expected
              + " argument(s) but got "
              + args.length
              + " for method "
              + method.getName());
    }

    Type[] genericTypes = method.getGenericParameterTypes();
    Object[] coerced = new Object[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i] == null) {
        coerced[i] = null;
        continue;
      }
      try {
        JavaType targetType = mapper.getTypeFactory().constructType(genericTypes[i]);
        // Skip conversion if the argument is already the right type.
        // This avoids lossy round-trips (e.g., OffsetDateTime losing its timezone offset).
        if (targetType.getRawClass().isInstance(args[i])) {
          coerced[i] = args[i];
          continue;
        }
        coerced[i] = mapper.convertValue(args[i], targetType);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Cannot convert argument "
                + i
                + " from "
                + args[i].getClass().getSimpleName()
                + " to "
                + genericTypes[i].getTypeName()
                + " for method "
                + method.getName()
                + ": "
                + e.getMessage(),
            e);
      }
    }
    return coerced;
  }
}
