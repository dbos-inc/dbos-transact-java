package dev.dbos.transact.internal;

import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSLifecycleListener;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.RegisteredWorkflowInstance;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
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

  @FunctionalInterface
  public interface RegisteredWorkflowConsumer {
    void register(Workflow wfTag, Object target, Method method, String instanceName);
  }

  private final Supplier<DBOSExecutor> executorSupplier;
  private final Consumer<DBOSLifecycleListener> listenerConsumer;
  private final RegisteredWorkflowConsumer workflowConsumer;

  public DBOSIntegration(
      @NonNull Supplier<DBOSExecutor> executorSupplier,
      @NonNull Consumer<DBOSLifecycleListener> lifecycleConsumer,
      @NonNull RegisteredWorkflowConsumer workflowConsumer) {
    this.executorSupplier = Objects.requireNonNull(executorSupplier);
    this.listenerConsumer = Objects.requireNonNull(lifecycleConsumer);
    this.workflowConsumer = Objects.requireNonNull(workflowConsumer);
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
   * Register a lifecycle listener that receives callbacks when DBOS is launched or shut down
   *
   * @param listener
   */
  public void registerLifecycleListener(@NonNull DBOSLifecycleListener listener) {
    listenerConsumer.accept(listener);
  }

  /**
   * Register a workflow method with DBOS. This method is used internally by the proxy registration
   * process and should not typically be called directly by application code.
   *
   * @param wfTag the Workflow annotation containing workflow configuration
   * @param target the object instance containing the workflow method
   * @param method the Method representing the workflow function
   * @param instanceName optional instance name for the workflow (can be null)
   * @throws IllegalStateException if called after DBOS is launched
   */
  public void registerWorkflow(
      @NonNull Workflow wfTag,
      @NonNull Object target,
      @NonNull Method method,
      @Nullable String instanceName) {
    workflowConsumer.register(wfTag, target, method, instanceName);
  }

  /**
   * Start or enqueue a workflow by its {@link RegisteredWorkflow} registration. Intended for use by
   * event listeners and other infrastructure that dispatches workflows by registration rather than
   * by direct invocation.
   *
   * @param regWorkflow the registered workflow to start; see {@link
   *     dev.dbos.transact.internal.DBOSIntegration#getRegisteredWorkflows()}
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

  /**
   * Get all workflows registered with DBOS.
   *
   * @return list of all registered workflow methods
   */
  public @NonNull Collection<RegisteredWorkflow> getRegisteredWorkflows() {
    return executor("getRegisteredWorkflows").getRegisteredWorkflows();
  }

  /**
   * Get all workflow instances registered with DBOS.
   *
   * @return list of all class instances containing registered workflow methods
   */
  public @NonNull Collection<RegisteredWorkflowInstance> getRegisteredWorkflowInstances() {
    return executor("getRegisteredWorkflowInstances").getRegisteredWorkflowInstances();
  }

  /**
   * Finds a registered workflow by its workflow name, class name, and instance name.
   *
   * @param workflowName the name of the workflow
   * @param className the name of the class containing the workflow
   * @return an Optional containing the RegisteredWorkflow if found, otherwise empty
   */
  public Optional<RegisteredWorkflow> getRegisteredWorkflow(
      @NonNull String workflowName, @NonNull String className) {
    return getRegisteredWorkflow(workflowName, className, "");
  }

  /**
   * Finds a registered workflow by its workflow name, class name, and instance name.
   *
   * @param workflowName the name of the workflow
   * @param className the name of the class containing the workflow
   * @param instanceName the name of the workflow instance
   * @return an Optional containing the RegisteredWorkflow if found, otherwise empty
   */
  public Optional<RegisteredWorkflow> getRegisteredWorkflow(
      @NonNull String workflowName, @NonNull String className, @NonNull String instanceName) {
    return executor("getRegisteredWorkflow")
        .getRegisteredWorkflow(workflowName, className, instanceName);
  }

  /**
   * Get a system database record stored by an external service A unique value is stored per
   * combination of service, workflowName, and key
   *
   * @param service Identity of the service maintaining the record
   * @param workflowName Fully qualified name of the workflow
   * @param key Key assigned within the service+workflow
   * @return Optional containing the value associated with the service+workflow+key combination, or
   *     empty if not found
   */
  public Optional<ExternalState> getExternalState(String service, String workflowName, String key) {
    return executor("getExternalState").getExternalState(service, workflowName, key);
  }

  /**
   * Insert or update a system database record stored by an external service A timestamped unique
   * value is stored per combination of service, workflowName, and key
   *
   * @param state ExternalState containing the service, workflow, key, and value to store
   * @return Value associated with the service+workflow+key combination, in case the stored value
   *     already had a higher version or timestamp
   */
  public ExternalState upsertExternalState(ExternalState state) {
    return executor("upsertExternalState").upsertExternalState(state);
  }
}
