package dev.dbos.transact.json;

import dev.dbos.transact.conductor.Conductor;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONUtil {
  private static final Logger logger = LoggerFactory.getLogger(Conductor.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  public static class JsonRuntimeException extends RuntimeException {
    public JsonRuntimeException(JsonProcessingException cause) {
      super(cause.getMessage(), cause);
      setStackTrace(cause.getStackTrace());
      for (Throwable suppressed : cause.getSuppressed()) {
        addSuppressed(suppressed);
      }
    }
  }

  static {
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // Optional
  }

  public static String serialize(Object obj) {
    return serializeArray(new Object[] {obj});
  }

  public static String serializeArray(Object[] args) {
    try {
      return mapper.writeValueAsString(new Boxed(args));
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  public static Object[] deserializeToArray(String json) {
    try {
      Boxed boxed = mapper.readValue(json, Boxed.class);
      return boxed.args;
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  public static String serializeAppException(Throwable error) {
    var wt = WireThrowableCodec.toWire(error, null, null);
    return serialize(wt);
  }

  public static WireThrowable deserializeAppExceptionWrapper(String str) {
    var wt = (WireThrowable) deserializeToArray(str)[0];
    return wt;
  }

  public static Throwable deserializeAppException(String str) {
    var wt = (WireThrowable) deserializeToArray(str)[0];
    try {
      return WireThrowableCodec.toThrowable(wt, null);
    } catch (Exception e) {
      logger.error(String.format("Couldn't deserialize %s", str));
      throw new RuntimeException(wt.message, e);
    }
  }

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

  public static final class WireThrowable {
    public int v = 1;
    public String type;
    public String message;
    public String causeType;
    public List<String> stackPreview;
    public int suppressedCount;
    public String base64bytes;
    public String node; // host/instance id
    public Map<String, Object> extra;
  }

  public static final class WireThrowableCodec {
    private static final int PREVIEW_FRAMES = 12;

    public static WireThrowable toWire(Throwable t, Map<String, Object> extra, String node) {
      WireThrowable w = new WireThrowable();
      w.type = t.getClass().getName();
      w.message = t.getMessage();
      w.causeType = (t.getCause() != null) ? t.getCause().getClass().getName() : null;
      w.suppressedCount = t.getSuppressed().length;
      w.stackPreview =
          Arrays.stream(t.getStackTrace())
              .limit(PREVIEW_FRAMES)
              .map(StackTraceElement::toString)
              .toList();
      w.node = node;
      w.extra = (extra == null) ? Map.of() : extra;

      byte[] javaSer = javaSerialize(t);
      String b64 = Base64.getEncoder().encodeToString(javaSer);
      w.base64bytes = b64;
      return w;
    }

    public static Throwable toThrowable(WireThrowable w, ClassLoader loader)
        throws IOException, ClassNotFoundException {
      if (w.base64bytes == null) throw new IllegalArgumentException("No serialized payload");

      byte[] javaSer = Base64.getDecoder().decode(w.base64bytes);

      try (var ois = new ObjectInputStream(new ByteArrayInputStream(javaSer))) {
        Object obj = ois.readObject();
        if (!(obj instanceof Throwable th)) {
          throw new StreamCorruptedException("Not a Throwable");
        }
        return th; // exact class restored
      }
    }

    private static byte[] javaSerialize(Object o) {
      try (var baos = new ByteArrayOutputStream();
          var oos = new ObjectOutputStream(baos)) {
        oos.writeObject(o);
        oos.flush();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
