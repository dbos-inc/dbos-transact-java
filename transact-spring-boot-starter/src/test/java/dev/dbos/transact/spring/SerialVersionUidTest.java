package dev.dbos.transact.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

/**
 * Guards the {@code serialVersionUID} of every serializable exception class in this module, as
 * {@code dev.dbos.transact.exceptions.SerialVersionUidTest} does in transact; its javadoc says what
 * to do when this fails. {@link WrappedThrowableException} is recorded as a step's error when a
 * step throws an {@link Error}, and replay unwraps it by catching it by class, so an error recorded
 * under another value no longer unwraps.
 */
public class SerialVersionUidTest {

  // The values as of 1.3.0, the last release.
  private static final Map<String, Long> VALUES =
      Map.of("dev.dbos.transact.spring.WrappedThrowableException", -3513833494768036443L);

  @Test
  public void noSerializableExceptionChangesItsSerialVersionUid() throws Exception {
    var location =
        Path.of(
            WrappedThrowableException.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .toURI());
    var loader = WrappedThrowableException.class.getClassLoader();
    var found = new TreeMap<String, Long>();
    // A classes directory, or the jar the Spring Boot 4 suite runs against.
    try (var jar = Files.isDirectory(location) ? null : FileSystems.newFileSystem(location)) {
      var root = jar == null ? location : jar.getPath("/");
      try (var files = Files.walk(root)) {
        for (var file : files.filter(f -> f.toString().endsWith(".class")).toList()) {
          var relative =
              root.relativize(file).toString().replace(file.getFileSystem().getSeparator(), ".");
          var name = relative.substring(0, relative.length() - ".class".length());
          var cls = Class.forName(name, false, loader);
          if (Throwable.class.isAssignableFrom(cls) && Serializable.class.isAssignableFrom(cls)) {
            found.put(name, ObjectStreamClass.lookup(cls).getSerialVersionUID());
          }
        }
      }
    }
    assertEquals(
        new TreeMap<>(VALUES),
        found,
        "See dev.dbos.transact.exceptions.SerialVersionUidTest's javadoc for what to do");
  }
}
