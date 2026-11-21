package dev.dbos.transact.json;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger logger = LoggerFactory.getLogger(WireThrowable.class);

  // The recorded classes an unreadable payload has been logged for at WARN.
  private static final Set<String> warnedTypes = ConcurrentHashMap.newKeySet();

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

  /**
   * Restores the recorded exception as its own class.
   *
   * <p>A payload this JVM cannot read back reads back as a {@link PortableWorkflowException}
   * instead. That happens when any class the payload holds is missing or can no longer read it: the
   * exception's own class, or one in its causes, suppressed exceptions or fields, whose {@code
   * serialVersionUID} or field types changed. It also happens when the payload is absent or
   * corrupt. So even a recorded JDK exception reads back this way when its cause's class is gone,
   * and a caller that catches the recorded class does not catch it.
   *
   * <p>The {@link PortableWorkflowException} carries:
   *
   * <ul>
   *   <li>as its {@link PortableWorkflowException#getErrorName() error name}, the recorded class's
   *       fully qualified name. It keeps that name when the error is recorded again, with either
   *       serialization, although a portable record of a live exception names its simple name;
   *   <li>as its message, the recorded message;
   *   <li>as its {@link PortableWorkflowException#getData() data}, the recorded stack preview and
   *       cause type;
   *   <li>as a {@link Throwable#getSuppressed() suppressed exception}, the failure to read the
   *       payload. It has no cause, so a workflow that lets it propagate records it, not the read
   *       failure, as its outcome. A failure that involves any class outside the JDK, such as one
   *       thrown by a class's own {@code readObject}, is replaced by an {@link
   *       InvalidObjectException} carrying its description and stack trace, since such a class may
   *       not serialize, and the error must be recordable again.
   * </ul>
   *
   * <p>The first unreadable payload of each recorded class is logged at WARN, later ones at DEBUG.
   */
  public Throwable toThrowable() {
    try {
      if (base64bytes == null) {
        throw new StreamCorruptedException("No serialized payload");
      }
      byte[] javaSer = Base64.getDecoder().decode(base64bytes);
      try (var ois = new ObjectInputStream(new ByteArrayInputStream(javaSer))) {
        Object obj = ois.readObject();
        if (obj instanceof Throwable th) {
          return th; // exact class restored
        }
        throw new StreamCorruptedException(
            "Not a Throwable: " + (obj == null ? "null" : obj.getClass().getName()));
      }
    } catch (IOException | ClassNotFoundException | RuntimeException | LinkageError e) {
      // A changed serialVersionUID or primitive field type throws InvalidClassException, a changed
      // reference field type ClassCastException, and a missing class ClassNotFoundException or
      // NoClassDefFoundError; a class's own readObject may throw anything.
      var text =
          "A recorded {} could not be read back, so it is returned as a"
              + " PortableWorkflowException: {}";
      if (warnedTypes.add(String.valueOf(type))) {
        logger.warn(text, type, e.toString());
      } else {
        logger.debug(text, type, e.toString());
      }
      return unreadable(e);
    }
  }

  private PortableWorkflowException unreadable(Throwable failure) {
    var data = new LinkedHashMap<String, Object>();
    if (causeType != null) {
      data.put("causeType", causeType);
    }
    if (stackPreview != null && !stackPreview.isEmpty()) {
      // Not List.copyOf, which refuses a null element.
      data.put("stackPreview", Collections.unmodifiableList(new ArrayList<>(stackPreview)));
    }
    var e =
        new PortableWorkflowException(
            message, type, null, data.isEmpty() ? null : Collections.unmodifiableMap(data));
    e.addSuppressed(fromTheJdk(failure) ? failure : describe(failure));
    return e;
  }

  // Whether the failure, its causes and suppressed exceptions are all JDK classes, which serialize.
  // One thrown by an application class's readObject may hold fields that do not.
  private static boolean fromTheJdk(Throwable failure) {
    var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
    var pending = new ArrayDeque<Throwable>();
    pending.add(failure);
    while (!pending.isEmpty()) {
      var t = pending.poll();
      if (!seen.add(t)) {
        continue;
      }
      if (!t.getClass().getName().startsWith("java.")) {
        return false;
      }
      if (t.getCause() != null) {
        pending.add(t.getCause());
      }
      pending.addAll(Arrays.asList(t.getSuppressed()));
    }
    return true;
  }

  private static InvalidObjectException describe(Throwable failure) {
    var described = new InvalidObjectException(failure.toString());
    described.setStackTrace(failure.getStackTrace());
    return described;
  }
}
