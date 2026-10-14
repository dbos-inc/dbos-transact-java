package dev.dbos.transact.txstep;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import dev.dbos.transact.exceptions.DBOSSystemDatabaseException;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

/**
 * Which failures a transactional step records as its result. Its own failures are; a refused
 * ownership check, or a failure to check ownership at all, are not: neither says anything about the
 * step.
 */
public class StepFailureClassificationTest {

  @Test
  public void theStepsOwnFailureIsRecorded() {
    assertNull(PostgresStepFactory.notTheStepsFailure(new IllegalStateException("app failure")));
  }

  @Test
  public void aConflictIsThrownUnrecorded() {
    var conflict = DBOSWorkflowExecutionConflictException.notOwned("wf");
    assertSame(conflict, PostgresStepFactory.notTheStepsFailure(conflict));
    // As a database library may hand it back, wrapped.
    assertSame(
        conflict,
        PostgresStepFactory.notTheStepsFailure(new RuntimeException("wrapped", conflict)));
  }

  @Test
  public void aFailureToCheckOwnershipIsThrownUnrecordedAsItself() {
    var unreachable = new DBOSSystemDatabaseException(new SQLException("gone", "08006"));
    var unknown = new PostgresStepFactory.OwnershipUnknownException(unreachable);
    assertSame(unreachable, PostgresStepFactory.notTheStepsFailure(unknown));
    assertSame(
        unreachable,
        PostgresStepFactory.notTheStepsFailure(new RuntimeException("wrapped", unknown)));
  }
}
