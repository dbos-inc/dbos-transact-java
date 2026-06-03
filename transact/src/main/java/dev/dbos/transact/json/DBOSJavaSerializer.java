package dev.dbos.transact.json;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.jsontype.PolymorphicTypeValidator;
import tools.jackson.databind.jsontype.impl.DefaultTypeResolverBuilder;

/**
 * Native Java serializer using Jackson with type information. This is the default serializer for
 * Java DBOS applications.
 */
public class DBOSJavaSerializer implements DBOSSerializer {

  public static final String NAME = "java_jackson";

  public static final DBOSJavaSerializer INSTANCE = new DBOSJavaSerializer();

  private final JsonMapper mapper;

  public DBOSJavaSerializer() {
    PolymorphicTypeValidator ptv =
        BasicPolymorphicTypeValidator.builder().allowIfBaseType(Object.class).build();

    DefaultTypeResolverBuilder typer =
        new DefaultTypeResolverBuilder(ptv, DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY, JsonTypeInfo.Id.CLASS, null) {
          @Override
          public boolean useForType(JavaType t) {
            return !t.isPrimitive();
          }
        };

    this.mapper =
        JsonMapper.builder()
            .setDefaultTyping(typer)
            .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String serialize(Object value) {
    return mapper.writeValueAsString(value);
  }

  @Override
  public Object deserialize(String text) {
    if (text == null) {
      return null;
    }
    return mapper.readValue(text, Object.class);
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
    var wt = mapper.readValue(text, WireThrowable.class);
    return wt.toThrowable();
  }
}
