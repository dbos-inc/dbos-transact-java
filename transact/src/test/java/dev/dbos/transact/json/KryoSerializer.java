package dev.dbos.transact.json;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Base64;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

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
  public String serialize(Object value) {
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
  public Object deserialize(String text) {
    if (text == null) {
      return null;
    }
    Kryo kryo = kryoLocal.get();
    byte[] bytes = Base64.getDecoder().decode(text);
    try (Input input = new Input(bytes)) {
      return kryo.readClassAndObject(input);
    }
  }

  // Kryo can't access private fields of java.lang.StackTraceElement in Java 17+ due to
  // strong module encapsulation, so throwables use Java's built-in serialization instead.

  @Override
  public String serializeThrowable(Throwable throwable) {
    try (var baos = new ByteArrayOutputStream();
        var oos = new ObjectOutputStream(baos)) {
      oos.writeObject(throwable);
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Throwable deserializeThrowable(String text) {
    if (text == null) {
      return null;
    }
    byte[] bytes = Base64.getDecoder().decode(text);
    try (var ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return (Throwable) ois.readObject();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
