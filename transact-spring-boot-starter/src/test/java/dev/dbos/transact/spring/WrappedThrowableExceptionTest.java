package dev.dbos.transact.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class WrappedThrowableExceptionTest {

  @Test
  void wrapsError() {
    var error = new AssertionError("test error");
    var wrapped = new WrappedThrowableException(error);
    assertSame(error, wrapped.getWrappedThrowable());
  }

  @Test
  void messageIncludesOriginalClassName() {
    var error = new OutOfMemoryError("oom");
    var wrapped = new WrappedThrowableException(error);
    assertEquals("Wrapped non-Exception throwable: OutOfMemoryError", wrapped.getMessage());
  }

  @Test
  void rejectsException() {
    var ex = new RuntimeException("not an error");
    var thrown =
        assertThrows(IllegalArgumentException.class, () -> new WrappedThrowableException(ex));
    assertNotNull(thrown);
  }
}
