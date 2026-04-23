package dev.dbos.transact.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtility {

  private static final ObjectMapper mapper = new ObjectMapper();

  public static String toJson(Object obj) {
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  public static <T> T fromJson(String content, Class<T> valueType) {
    try {
      return mapper.readValue(content, valueType);
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }
}
