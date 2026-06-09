package dev.dbos.transact.txstep;

/**
 * Options for a transactional step executed via a step factory.
 *
 * @param name the stable step name used for idempotency tracking within the workflow
 * @param isolationLevel the transaction isolation level to request; {@link IsolationLevel#DEFAULT}
 *     leaves the connection's level unchanged
 */
public record StepFactoryOptions(String name, IsolationLevel isolationLevel) {

  /** Creates options with {@link IsolationLevel#DEFAULT} (no override). */
  public StepFactoryOptions(String name) {
    this(name, IsolationLevel.DEFAULT);
  }
}
