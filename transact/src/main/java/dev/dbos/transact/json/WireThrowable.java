package dev.dbos.transact.json;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;

record WireThrowable(
    int v,
    String type,
    String message,
    String causeType,
    List<String> stackPreview,
    int suppressedCount,
    String base64bytes,
    String node, // host/instance id
    Map<String, Object> extra) {

  private static final int PREVIEW_FRAMES = 12;

  public WireThrowable {
    if (v != 1) {
      throw new IllegalArgumentException("Unsupported WireThrowable version: " + v);
    }
  }

  public static WireThrowable fromThrowable(
      Throwable throwable, Map<String, Object> extra, String node) {
    byte[] serializedThrowable;
    try (var baOut = new ByteArrayOutputStream();
        var out = new ObjectOutputStream(baOut)) {
      out.writeObject(throwable);
      out.flush();
      serializedThrowable = baOut.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return new WireThrowable(
        1,
        throwable.getClass().getName(),
        throwable.getMessage(),
        throwable.getCause() != null ? throwable.getCause().getClass().getName() : null,
        Arrays.stream(throwable.getStackTrace())
            .limit(PREVIEW_FRAMES)
            .map(StackTraceElement::toString)
            .toList(),
        throwable.getSuppressed().length,
        Base64.getEncoder().encodeToString(serializedThrowable),
        node,
        Objects.requireNonNullElseGet(extra, Map::of));
  }

  public Throwable toThrowable() {
    if (base64bytes == null) {
      throw new IllegalArgumentException("No serialized payload");
    }
    byte[] javaSer = Base64.getDecoder().decode(base64bytes);

    try (var ois = new ObjectInputStream(new ByteArrayInputStream(javaSer))) {
      Object obj = ois.readObject();
      if (obj instanceof Throwable th) {
        return th; // exact class restored
      } else {
        throw new StreamCorruptedException("Not a Throwable");
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}
