package dev.dbos.transact.json;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
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
    PolymorphicTypeValidator ptv =
        BasicPolymorphicTypeValidator.builder().allowIfBaseType(Object.class).build();
    var typer =
        new ObjectMapper.DefaultTypeResolverBuilder(ObjectMapper.DefaultTyping.NON_FINAL, ptv) {
          @Override
          public boolean useForType(JavaType t) {
            return !t.isPrimitive();
          }
        };
    typer.init(JsonTypeInfo.Id.CLASS, null);
    typer.inclusion(JsonTypeInfo.As.PROPERTY);

    this.mapper =
        new ObjectMapper()
            .setDefaultTyping(typer)
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String serialize(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  @Override
  public Object deserialize(String text) {
    if (text == null) {
      return null;
    }
    try {
      return mapper.readValue(text, Object.class);
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  @Override
  public String serializeThrowable(Throwable throwable) {
    if (throwable == null) {
      return null;
    }
    var wt = WireThrowable.fromThrowable(throwable, null, null);
    return serialize(wt);
  }

  @Override
  public Throwable deserializeThrowable(String text) {
    if (text == null) {
      return null;
    }
    try {
      var wt = mapper.readValue(text, WireThrowable.class);
      return wt.toThrowable();
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }
}
