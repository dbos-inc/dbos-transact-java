package dev.dbos.transact.spring.txstep;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.transaction.annotation.Isolation;

/**
 * Marks a Spring-managed method as an idempotent transactional step that integrates with the DBOS
 * runtime.
 *
 * <p>When called inside a {@link dev.dbos.transact.workflow.Workflow @Workflow}, the method body
 * runs inside a {@code REQUIRES_NEW} Spring transaction. The step output is written to the {@code
 * tx_step_outputs} table atomically with the user's database work. On workflow retry, the recorded
 * output is replayed without re-executing the method body.
 *
 * <p>The annotated method must be called through a Spring proxy (e.g. a self-injected reference)
 * for the aspect to intercept it.
 *
 * <p>Example:
 *
 * <pre>{@code
 * @TransactionalStep
 * public Order saveOrder(Order order) {
 *     return repository.save(order);
 * }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransactionalStep {
  /** Stable name for this step within the workflow. Defaults to the method name when left empty. */
  String name() default "";

  /**
   * Transaction isolation level for this step. {@link Isolation#DEFAULT} leaves the
   * datasource/connection-pool default unchanged.
   */
  Isolation isolationLevel() default Isolation.DEFAULT;
}
