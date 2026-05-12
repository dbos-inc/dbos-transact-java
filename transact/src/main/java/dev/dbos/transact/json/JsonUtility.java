package dev.dbos.transact.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class JsonUtility {

  public static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

  private JsonUtility() {}

  public static String toJson(Object obj) {
    try {
      return MAPPER.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  public static <T> T fromJson(String content, Class<T> valueType) {
    try {
      return MAPPER.readValue(content, valueType);
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  public static <T> T fromJson(InputStream in, Class<T> valueType) {
    try {
      return MAPPER.readValue(in, valueType);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T fromJson(InputStream in, TypeReference<T> valueType) {
    try {
      return MAPPER.readValue(in, valueType);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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
