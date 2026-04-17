package dev.dbos.transact.internal;

import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.function.Supplier;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Internal DBOS APIs for use by specialized integrations such as AOP aspects and event listeners.
 *
 * <p>This class is <strong>not part of the primary public API</strong>. It is public so that code
 * in other packages (e.g. {@code dev.dbos.transact.spring}) can access it, but it may change
 * without notice. Application code should use {@link dev.dbos.transact.DBOS} instead.
 *
 * <p>Obtain an instance via {@link dev.dbos.transact.DBOS#integration()}.
 */
public class DBOSIntegration {
  private final Supplier<DBOSExecutor> executorSupplier;

  public DBOSIntegration(@NonNull Supplier<DBOSExecutor> executorSupplier) {
    this.executorSupplier = Objects.requireNonNull(executorSupplier);
  }

  private DBOSExecutor executor(String caller) {
    var exec = executorSupplier.get();
    if (exec == null) {
      throw new IllegalStateException(
          "DBOS is not launched. Cannot call %s before launch.".formatted(caller));
    }
    return exec;
  }

  /**
   * Start or enqueue a workflow by its {@link RegisteredWorkflow} registration. Intended for use by
   * event listeners and other infrastructure that dispatches workflows by registration rather than
   * by direct invocation.
   *
   * @param regWorkflow the registered workflow to start; see {@link
   *     dev.dbos.transact.DBOS#getRegisteredWorkflows()}
   * @param args arguments to pass to the workflow function
   * @param options execution options such as workflow ID, queue, and timeout; may be {@code null}
   *     to use defaults
   * @return a handle to the running or enqueued workflow
   * @throws IllegalStateException if DBOS has not been launched
   */
  public WorkflowHandle<?, ?> startRegisteredWorkflow(
      @NonNull RegisteredWorkflow regWorkflow,
      @NonNull Object[] args,
      @Nullable StartWorkflowOptions options) {
    return executor("startRegisteredWorkflow").startRegisteredWorkflow(regWorkflow, args, options);
  }

  /**
   * Execute a workflow method via its reflective {@link Method} handle. Intended for use by AOP
   * interceptors that capture workflow invocations at the proxy boundary.
   *
   * @param target the object instance on which the workflow method is declared
   * @param instanceName the DBOS instance name for {@code target}, or {@code null} for the default
   * @param method the workflow {@link Method} to invoke
   * @param args arguments to pass to the workflow method
   * @param wfTag the {@link Workflow} annotation present on {@code method}
   * @return the workflow's return value
   * @throws Exception if the workflow throws a checked exception
   * @throws IllegalStateException if DBOS has not been launched
   */
  public Object runWorkflow(
      Object target, String instanceName, Method method, Object[] args, Workflow wfTag)
      throws Exception {
    return executor("runWorkflow").runWorkflow(target, instanceName, method, args, wfTag);
  }
}
