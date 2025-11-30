package dev.dbos.transact.json;

import dev.dbos.transact.json.JSONUtil.JsonRuntimeException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class JacksonJSONSerde implements JSONSerde {
  private final ObjectMapper mapper = new ObjectMapper();

  {
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // Optional
  }

  public String serializeArray(Object[] args) {
    try {
      return mapper.writeValueAsString(new Boxed(args));
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  public Object[] deserializeToArray(String json) {
    try {
      Boxed boxed = mapper.readValue(json, Boxed.class);
      return boxed.args;
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  public String toJson(Object obj) {
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  public <T> T fromJson(String content, Class<T> valueType) {
    try {
      return mapper.readValue(content, valueType);
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }
}
