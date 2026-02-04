package dev.dbos.transact.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Native Java serializer using Jackson with type information. This is the default serializer for
 * Java DBOS applications.
 */
public class DBOSJavaSerializer implements DBOSSerializer {

  public static final String NAME = "java_jackson";

  public static final DBOSJavaSerializer INSTANCE = new DBOSJavaSerializer();

  private final ObjectMapper mapper;

  public DBOSJavaSerializer() {
    this.mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String stringify(Object value) {
    try {
      return mapper.writeValueAsString(new Boxed(new Object[] {value}));
    } catch (JsonProcessingException e) {
      throw new JSONUtil.JsonRuntimeException(e);
    }
  }

  @Override
  public Object parse(String text) {
    if (text == null) {
      return null;
    }
    try {
      Boxed boxed = mapper.readValue(text, Boxed.class);
      return boxed.args != null && boxed.args.length > 0 ? boxed.args[0] : null;
    } catch (JsonProcessingException e) {
      throw new JSONUtil.JsonRuntimeException(e);
    }
  }

  /** Serialize an array of values (for workflow arguments). */
  public String stringifyArray(Object[] values) {
    try {
      return mapper.writeValueAsString(new Boxed(values));
    } catch (JsonProcessingException e) {
      throw new JSONUtil.JsonRuntimeException(e);
    }
  }

  /** Deserialize to an array of values (for workflow arguments). */
  public Object[] parseArray(String text) {
    if (text == null) {
      return null;
    }
    try {
      Boxed boxed = mapper.readValue(text, Boxed.class);
      return boxed.args;
    } catch (JsonProcessingException e) {
      throw new JSONUtil.JsonRuntimeException(e);
    }
  }
}
