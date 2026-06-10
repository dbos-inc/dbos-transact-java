package dev.dbos.transact.json;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

public final class JsonUtility {

  public static final JsonMapper MAPPER =
      JsonMapper.builder()
          .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
          .enable(DateTimeFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
          .build();

  private JsonUtility() {}

  public static String toJson(Object obj) {
    return MAPPER.writeValueAsString(obj);
  }

  public static <T> T fromJson(String content, Class<T> valueType) {
    return MAPPER.readValue(content, valueType);
  }

  public static <T> T fromJson(InputStream in, Class<T> valueType) {
    return MAPPER.readValue(in, valueType);
  }

  public static <T> T fromJson(InputStream in, TypeReference<T> valueType) {
    return MAPPER.readValue(in, valueType);
  }

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
        JavaType targetType = MAPPER.getTypeFactory().constructType(genericTypes[i]);
        if (targetType.getRawClass().isInstance(args[i])) {
          coerced[i] = args[i];
          continue;
        }
        coerced[i] = MAPPER.convertValue(args[i], targetType);
      } catch (RuntimeException e) {
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
