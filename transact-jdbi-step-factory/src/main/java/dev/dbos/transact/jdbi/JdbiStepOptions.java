package dev.dbos.transact.jdbi;

import org.jdbi.v3.core.transaction.TransactionIsolationLevel;

/**
 * Options for a transactional step executed via {@link JdbiStepFactory}.
 *
 * @param name the stable step name used for idempotency tracking within the workflow
 * @param isolationLevel the transaction isolation level to request; {@code null} leaves the
 *     connection's level unchanged
 */
public record JdbiStepOptions(String name, TransactionIsolationLevel isolationLevel) {

  /** Creates options with no isolation level override. */
  public JdbiStepOptions(String name) {
    this(name, null);
  }
}
