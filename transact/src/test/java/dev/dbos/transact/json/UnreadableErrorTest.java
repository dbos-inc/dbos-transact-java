package dev.dbos.transact.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException;
import dev.dbos.transact.utils.LogCapture;
import dev.dbos.transact.workflow.ErrorResult;

import java.io.IOException;
import java.io.InvalidClassException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import ch.qos.logback.classic.Level;
import org.junit.jupiter.api.Test;

/** A recorded error that can't be read back as its own class reads back as a portable one. */
public class UnreadableErrorTest {

  /** Serializable, and seen by no other test. */
  public static class GoneException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public GoneException(String message) {
      super(message);
    }

    public GoneException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** Holds a field Java serialization can't write, so it can't be recorded itself. */
  public static class HoldsALockException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final Object lock = new Object();

    public HoldsALockException(String message) {
      super(message);
    }
  }

  /** Records fine, and throws an exception that can't be recorded when read back. */
  public static class RefusesToReadException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public RefusesToReadException(String message) {
      super(message);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      throw new HoldsALockException("refused");
    }
  }

  /** Records fine, and fails to link a class when read back. */
  public static class FailsToLinkException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public FailsToLinkException(String message) {
      super(message);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      throw new NoClassDefFoundError("dev/dbos/transact/json/NoSuchHelper");
    }
  }

  @Test
  public void aRecordWithAnotherSerialVersionUidReadsBackAsPortable() throws Exception {
    // The value 1.1.x computed for this class, before 1.2.0 changed its shape.
    long older = -1810941168465248470L;
    var error = new DBOSWorkflowFunctionNotFoundException("wf-1", "missingWorkflow");
    var recorded =
        withSerialVersionUid(
            WireThrowable.fromThrowable(error, null, null), error.getClass(), older);

    var read = recorded.toThrowable();

    var portable = assertInstanceOf(PortableWorkflowException.class, read);
    assertEquals(DBOSWorkflowFunctionNotFoundException.class.getName(), portable.getErrorName());
    assertEquals(error.getMessage(), portable.getMessage());
    assertInstanceOf(InvalidClassException.class, portable.getCause());
    var data = assertInstanceOf(Map.class, portable.getData());
    var preview = assertInstanceOf(List.class, data.get("stackPreview"));
    assertEquals(recorded.stackPreview(), preview);
    assertFalse(data.containsKey("causeType"));
  }

  @Test
  public void aRecordWhoseClassIsMissingReadsBackAsPortable() throws Exception {
    var error = new GoneException("gone", new IllegalStateException("why"));
    var recorded = WireThrowable.fromThrowable(error, null, null);
    // Rename the class in the payload to one that does not exist, keeping its length.
    var name = GoneException.class.getName();
    var missing = name.substring(0, name.length() - 4) + "Lost";
    var renamed =
        replace(
            decode(recorded),
            name.getBytes(StandardCharsets.UTF_8),
            missing.getBytes(StandardCharsets.UTF_8));

    var read = withPayload(recorded, renamed).toThrowable();

    var portable = assertInstanceOf(PortableWorkflowException.class, read);
    assertEquals(name, portable.getErrorName());
    assertEquals("gone", portable.getMessage());
    assertInstanceOf(ClassNotFoundException.class, portable.getCause());
    var data = assertInstanceOf(Map.class, portable.getData());
    assertEquals(IllegalStateException.class.getName(), data.get("causeType"));
  }

  @Test
  public void aCorruptRecordReadsBackAsPortable() throws Exception {
    var recorded = WireThrowable.fromThrowable(new GoneException("corrupt"), null, null);
    var read = withPayload(recorded, new byte[] {1, 2, 3}).toThrowable();

    var portable = assertInstanceOf(PortableWorkflowException.class, read);
    assertEquals(GoneException.class.getName(), portable.getErrorName());
    assertInstanceOf(IOException.class, portable.getCause());
  }

  @Test
  public void aRecordWhoseCauseClassIsMissingReadsBackAsPortable() throws Exception {
    var error = new IllegalStateException("top", new GoneException("below"));
    var recorded = WireThrowable.fromThrowable(error, null, null);
    var name = GoneException.class.getName();
    var missing = name.substring(0, name.length() - 4) + "Lost";
    var renamed =
        replace(
            decode(recorded),
            name.getBytes(StandardCharsets.UTF_8),
            missing.getBytes(StandardCharsets.UTF_8));

    var read = withPayload(recorded, renamed).toThrowable();

    // The top-level class exists, but the record can't be read without its cause's.
    var portable = assertInstanceOf(PortableWorkflowException.class, read);
    assertEquals(IllegalStateException.class.getName(), portable.getErrorName());
    assertEquals("top", portable.getMessage());
    assertInstanceOf(ClassNotFoundException.class, portable.getCause());
    var data = assertInstanceOf(Map.class, portable.getData());
    assertEquals(name, data.get("causeType"));
  }

  @Test
  public void aRecordWithNoPayloadReadsBackAsPortable() {
    var recorded = WireThrowable.fromThrowable(new GoneException("empty"), null, null);
    var empty =
        new WireThrowable(
            recorded.v(),
            recorded.type(),
            recorded.message(),
            recorded.causeType(),
            recorded.stackPreview(),
            recorded.suppressedCount(),
            null,
            recorded.node(),
            recorded.extra());

    var portable = assertInstanceOf(PortableWorkflowException.class, empty.toThrowable());
    assertEquals(GoneException.class.getName(), portable.getErrorName());
    assertEquals("empty", portable.getMessage());
    assertInstanceOf(StreamCorruptedException.class, portable.getCause());
  }

  @Test
  public void aFailureThatCantBeRecordedIsReplacedByItsDescription() {
    var recorded = WireThrowable.fromThrowable(new RefusesToReadException("kept"), null, null);

    var portable = assertInstanceOf(PortableWorkflowException.class, recorded.toThrowable());

    assertEquals(RefusesToReadException.class.getName(), portable.getErrorName());
    var cause = assertInstanceOf(InvalidObjectException.class, portable.getCause());
    assertTrue(
        cause.getMessage().contains(HoldsALockException.class.getName()), cause.getMessage());
    assertTrue(cause.getMessage().contains("refused"), cause.getMessage());
    // Recorded again, as when a workflow lets the replayed error propagate, and read back.
    var again =
        DBOSJavaSerializer.INSTANCE.deserializeThrowable(
            DBOSJavaSerializer.INSTANCE.serializeThrowable(portable));
    var reread = assertInstanceOf(PortableWorkflowException.class, again);
    assertEquals(RefusesToReadException.class.getName(), reread.getErrorName());
    assertEquals("kept", reread.getMessage());
  }

  @Test
  public void aLinkageErrorFromTheJdkIsKeptAsTheCause() {
    var recorded = WireThrowable.fromThrowable(new FailsToLinkException("linked"), null, null);

    var portable = assertInstanceOf(PortableWorkflowException.class, recorded.toThrowable());

    assertEquals(FailsToLinkException.class.getName(), portable.getErrorName());
    assertInstanceOf(NoClassDefFoundError.class, portable.getCause());
  }

  @Test
  public void anUnreadableRecordCanBeRecordedAgainWithEitherSerialization() throws Exception {
    var error = new DBOSWorkflowFunctionNotFoundException("wf-3", "missingWorkflow");
    var recorded =
        withSerialVersionUid(WireThrowable.fromThrowable(error, null, null), error.getClass(), 7L);
    var portable = assertInstanceOf(PortableWorkflowException.class, recorded.toThrowable());

    var java =
        DBOSJavaSerializer.INSTANCE.deserializeThrowable(
            DBOSJavaSerializer.INSTANCE.serializeThrowable(portable));
    var fromJava = assertInstanceOf(PortableWorkflowException.class, java);
    assertEquals(DBOSWorkflowFunctionNotFoundException.class.getName(), fromJava.getErrorName());
    assertEquals(error.getMessage(), fromJava.getMessage());
    assertEquals(portable.getData(), fromJava.getData());
    assertInstanceOf(InvalidClassException.class, fromJava.getCause());

    // The portable record keeps the fully qualified name.
    var text = DBOSPortableSerializer.INSTANCE.serializeThrowable(portable);
    var fromPortable =
        assertInstanceOf(
            PortableWorkflowException.class,
            DBOSPortableSerializer.INSTANCE.deserializeThrowable(text));
    assertEquals(
        DBOSWorkflowFunctionNotFoundException.class.getName(), fromPortable.getErrorName());
    assertEquals(error.getMessage(), fromPortable.getMessage());
  }

  @Test
  public void theFirstUnreadableRecordOfAClassIsLoggedAtWarn() throws Exception {
    var recorded = WireThrowable.fromThrowable(new GoneException("logged"), null, null);
    // A class name no other test records, so no earlier read has logged it.
    var type = "dev.dbos.transact.json.OnlyLoggedHere";
    var unreadable =
        new WireThrowable(
            recorded.v(),
            type,
            recorded.message(),
            recorded.causeType(),
            recorded.stackPreview(),
            recorded.suppressedCount(),
            Base64.getEncoder().encodeToString(new byte[] {1, 2, 3}),
            recorded.node(),
            recorded.extra());

    try (var logs = new LogCapture(WireThrowable.class)) {
      for (int i = 0; i < 3; i++) {
        assertInstanceOf(PortableWorkflowException.class, unreadable.toThrowable());
      }
      assertEquals(1, logs.messages(Level.WARN, type).size());
    }
  }

  @Test
  public void anUnreadableRecordKeepsItsClassNameInTheErrorResult() throws Exception {
    var error = new DBOSWorkflowFunctionNotFoundException("wf-2", "missingWorkflow");
    var recorded =
        withSerialVersionUid(WireThrowable.fromThrowable(error, null, null), error.getClass(), 42L);
    var text = DBOSJavaSerializer.INSTANCE.serialize(recorded);

    var result = ErrorResult.deserialize(text, DBOSJavaSerializer.NAME, null);

    assertEquals(DBOSWorkflowFunctionNotFoundException.class.getName(), result.className());
    assertEquals(error.getMessage(), result.message());
    assertInstanceOf(PortableWorkflowException.class, result.throwable());
  }

  @Test
  public void aReadableRecordIsRestoredAsItsOwnClass() {
    var error = new GoneException("here");
    var read = WireThrowable.fromThrowable(error, null, null).toThrowable();
    var restored = assertInstanceOf(GoneException.class, read);
    assertEquals("here", restored.getMessage());
    assertNull(restored.getCause());
  }

  private static byte[] decode(WireThrowable recorded) {
    return Base64.getDecoder().decode(recorded.base64bytes());
  }

  private static WireThrowable withPayload(WireThrowable recorded, byte[] payload) {
    return new WireThrowable(
        recorded.v(),
        recorded.type(),
        recorded.message(),
        recorded.causeType(),
        recorded.stackPreview(),
        recorded.suppressedCount(),
        Base64.getEncoder().encodeToString(payload),
        recorded.node(),
        recorded.extra());
  }

  /** Rewrites the serialVersionUID that follows the class's name in its class descriptor. */
  private static WireThrowable withSerialVersionUid(
      WireThrowable recorded, Class<?> cls, long uid) {
    var bytes = decode(recorded);
    var name = cls.getName().getBytes(StandardCharsets.UTF_8);
    // A class descriptor writes the name as modified UTF-8, a two-byte length then the bytes,
    // followed by the eight-byte serialVersionUID.
    var at = indexOf(bytes, name);
    ByteBuffer.wrap(bytes, at + name.length, Long.BYTES).putLong(uid);
    return withPayload(recorded, bytes);
  }

  private static byte[] replace(byte[] bytes, byte[] from, byte[] to) {
    var at = indexOf(bytes, from);
    System.arraycopy(to, 0, bytes, at, to.length);
    return bytes;
  }

  private static int indexOf(byte[] bytes, byte[] part) {
    outer:
    for (int i = 0; i + part.length <= bytes.length; i++) {
      for (int j = 0; j < part.length; j++) {
        if (bytes[i + j] != part[j]) {
          continue outer;
        }
      }
      return i;
    }
    throw new AssertionError("not found in the payload");
  }
}
