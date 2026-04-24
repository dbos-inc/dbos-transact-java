package dev.dbos.transact.json;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;
import java.util.Base64;

/**
 * Example custom DBOSSerializer implementation using Kryo.
 *
 * <p>Kryo instances are not thread-safe, so each thread gets its own via ThreadLocal. Values are
 * serialized to bytes then Base64-encoded for storage as strings.
 */
public class KryoSerializer implements DBOSSerializer {

  public static final String NAME = "kryo";

  public static final KryoSerializer INSTANCE = new KryoSerializer();

  private static final ThreadLocal<Kryo> kryoLocal =
      ThreadLocal.withInitial(
          () -> {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            return kryo;
          });

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String stringify(Object value) {
    Kryo kryo = kryoLocal.get();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos)) {
      kryo.writeClassAndObject(output, value);
      output.flush();
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException("Kryo serialization failed", e);
    }
  }

  @Override
  public Object parse(String text) {
    if (text == null) {
      return null;
    }
    Kryo kryo = kryoLocal.get();
    byte[] bytes = Base64.getDecoder().decode(text);
    try (Input input = new Input(bytes)) {
      return kryo.readClassAndObject(input);
    }
  }

  @Override
  public String stringifyThrowable(Throwable throwable) {
    return stringify(throwable);
  }

  @Override
  public Throwable parseThrowable(String text) {
    if (text == null) {
      return null;
    }
    return (Throwable) parse(text);
  }
}
