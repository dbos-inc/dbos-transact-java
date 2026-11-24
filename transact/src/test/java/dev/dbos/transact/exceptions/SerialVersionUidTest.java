package dev.dbos.transact.exceptions;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;

import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

/**
 * Guards the {@code serialVersionUID} of every serializable exception class compiled from this
 * module's Java sources.
 *
 * <p>Recorded errors (a step's error, a failed workflow's outcome) are stored with Java
 * serialization. It writes the class's {@code serialVersionUID} into the record, and refuses to
 * read the record back when the class's value differs. A class that declares none gets a value
 * computed from its shape: its name, modifiers, interfaces, fields, and non-private constructors
 * and methods. So an edit as small as adding a constructor or a getter changes the value, and every
 * error of that class already recorded stops reading back as that class.
 *
 * <p>{@link #VALUES} holds each class's value as of 1.3.0; rows marked unreleased are classes no
 * release has shipped yet. When a release ships, drop the marks and update the version here. When
 * this test fails:
 *
 * <ul>
 *   <li><b>A class's value changed.</b> Do not update the table. Declare the value from the table
 *       in the class, {@code private static final long serialVersionUID = <value>L;}, so the errors
 *       earlier releases recorded still read back. Change a released class's value only to refuse
 *       its old records on purpose, and say so in the release notes. An unreleased class can just
 *       take its new value in the table.
 *   <li><b>A new class.</b> Add a row, marked unreleased, with the value the failure reports, or
 *       declare {@code serialVersionUID = 1L} in the class and add a row with {@code 1L}.
 *   <li><b>A class was removed or renamed.</b> Remove its row. Its recorded errors now read back as
 *       a {@link dev.dbos.transact.json.PortableWorkflowException} carrying the old class name.
 * </ul>
 *
 * <p>A declared value keeps old records readable only across compatible changes: Java serialization
 * gives a field an old record lacks its default value, and skips a field the class no longer has.
 * This test cannot see the changes that still break old records, so avoid them in a released class,
 * or treat them as refusing its old records:
 *
 * <ul>
 *   <li>changing a field's type, which fails the read;
 *   <li>moving a field to another class in the hierarchy, such as a new superclass, which reads
 *       back as its default value, silently;
 *   <li>removing or renaming a constant of an enum a field holds, which fails the read;
 *   <li>changing the class's superclass, or whether it is an enum or a record.
 * </ul>
 */
public class SerialVersionUidTest {

  static final Map<String, Long> VALUES =
      Map.ofEntries(
          Map.entry(
              "dev.dbos.transact.database.SystemDatabase$ConnectionUnavailableException",
              -3370422481300055918L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSApplicationNameConflictException",
              316016100886081188L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException",
              8112519209661993935L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSConflictingWorkflowException",
              -2143452395330365997L),
          Map.entry("dev.dbos.transact.exceptions.DBOSDelayPastLatestInstantException", 1L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException",
              3444752521679854729L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException",
              -8193459193556191687L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSQueueDuplicatedException", 4474367735597919503L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSStepNondeterminismException", 7359312088822354029L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSSystemDatabaseException", -7424815834835183831L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSUnexpectedStepException", -6212194037557396383L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSWorkflowCancelledException", 2890892167991354316L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException",
              210173734361363888L),
          Map.entry(
              "dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException",
              -7685608188535248792L),
          Map.entry("dev.dbos.transact.exceptions.DBOSWorkflowIdInUseException", 1L),
          // Unreleased.
          Map.entry("dev.dbos.transact.exceptions.DBOSWorkflowSuspendedException", 1L),
          // Unreleased.
          Map.entry("dev.dbos.transact.internal.Entitlements$InvalidUrl", -1052318094765006001L),
          // Unreleased.
          Map.entry(
              "dev.dbos.transact.internal.Entitlements$MalformedAnswer", 3705209497722782822L),
          Map.entry("dev.dbos.transact.json.PortableWorkflowException", -2972485864766567000L),
          Map.entry(
              "dev.dbos.transact.txstep.PostgresStepFactory$OwnershipUnknownException",
              5789552372345926480L),
          Map.entry(
              "dev.dbos.transact.txstep.PostgresStepFactory$StepConflictException",
              -4773464005175304708L));

  @Test
  public void noSerializableExceptionChangesItsSerialVersionUid() throws Exception {
    var problems = new ArrayList<String>();
    var found = serializableThrowables(DBOS.class);
    found.forEach(
        (name, value) -> {
          var expected = VALUES.get(name);
          if (expected == null) {
            problems.add(name + " is not in the table; its value is " + value + "L");
          } else if (expected.longValue() != value) {
            problems.add(name + " is " + value + "L; the table has " + expected + "L");
          }
        });
    VALUES.keySet().stream()
        .filter(name -> !found.containsKey(name))
        .sorted()
        .forEach(name -> problems.add(name + " is in the table but no longer exists"));
    assertTrue(
        problems.isEmpty(),
        "See SerialVersionUidTest's javadoc for what to do:\n" + String.join("\n", problems));
  }

  /**
   * Every serializable Throwable compiled into the classes directory {@code anchor} was loaded
   * from, with its serialVersionUID, declared or computed.
   */
  static Map<String, Long> serializableThrowables(Class<?> anchor) throws Exception {
    var location = Path.of(anchor.getProtectionDomain().getCodeSource().getLocation().toURI());
    var loader = anchor.getClassLoader();
    var found = new TreeMap<String, Long>();
    // A classes directory, or the jar a test suite runs against.
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
    assertTrue(!found.isEmpty(), "no exception classes found under " + location);
    return found;
  }
}
