package dev.dbos.transact.suspend;

import dev.dbos.transact.conductor.ConductorSystemDatabase;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/**
 * What suspend mode in {@code dev.dbos:transact-enterprise} may call on a launched DBOS instance.
 * It is not application API.
 *
 * <p>An enterprise build runs against any later DBOS Transact build of the same minor version, so
 * within a minor this interface only gains methods: none is changed or removed.
 */
public interface SuspendEngine {
  /** The system database suspend mode's SQL runs against. */
  ConductorSystemDatabase systemDatabase();

  /**
   * The application whose workflows this instance's background pass re-enqueues, or {@code null}
   * for an instance with no application name, which sees every application's.
   */
  @Nullable String appName();

  /**
   * What a recv recorded under {@code stepId}, if it has run.
   *
   * @throws dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException if {@code ownerXid}
   *     no longer owns the workflow
   */
  Optional<RecordedRecv> recordedRecv(String workflowId, int stepId, String ownerXid)
      throws SQLException;

  /**
   * The instant a recv's timeout runs out, recorded under {@code timeoutStepId} the first time and
   * read back by every replay.
   */
  Instant recvDeadline(String workflowId, int timeoutStepId, Duration timeout, String ownerXid)
      throws SQLException;

  /**
   * The instant a sleep ends, recorded under {@code functionId} the first time and read back by
   * every replay. A sleep whose end cannot be stored is refused, and the refusal recorded and
   * thrown again on replay.
   */
  Instant sleepEnd(String workflowId, int functionId, Duration duration, String ownerXid)
      throws SQLException;

  /**
   * The ordinary recv, which waits in this thread. Suspend mode falls back to it when a message is
   * already waiting or the timeout has passed, so the step is recorded as any recv records it.
   */
  @Nullable Object recv(
      String workflowId,
      int stepId,
      int timeoutStepId,
      @Nullable String topic,
      Duration timeout,
      String ownerXid);

  /**
   * Locks the workflow's status row for the rest of {@code connection}'s transaction, and fails
   * unless the execution holding {@code ownerXid} still owns the workflow. A parked workflow is
   * released by the same transaction, so no hand-off can land between the check and the write.
   *
   * @throws dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException if it does not
   */
  void checkOwner(Connection connection, String workflowId, String ownerXid) throws SQLException;

  /** SQL for the system database's clock now, in epoch milliseconds. */
  String clockEpochMsSql();

  /**
   * Whether the system database is CockroachDB, which has no advisory locks: work that would take
   * one to keep its processes from doing the same thing at once must run without it there.
   */
  boolean isCockroach();
}
