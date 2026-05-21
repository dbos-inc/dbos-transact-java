package dev.dbos.transact.internal;

import dev.dbos.transact.Constants;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSLifecycleListener;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.RegisteredWorkflowInstance;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.internal.DebouncerService;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

  /**
   * Callback used during workflow registration to process each discovered workflow method.
   * Implementations receive the {@link Workflow} annotation, the target object, the reflective
   * {@link Method}, and the optional instance name.
   */
  @FunctionalInterface
  public interface RegisteredWorkflowConsumer {
    void register(Workflow wfTag, Object target, Method method, String instanceName);
  }

  private final DBOSConfig config;
  private final WorkflowRegistry workflowRegistry;
  private final Supplier<DBOSExecutor> executorSupplier;
  private final Consumer<DBOSLifecycleListener> listenerConsumer;

  public DBOSIntegration(
      @NonNull DBOSConfig config,
      @NonNull WorkflowRegistry workflowRegistry,
      @NonNull Supplier<DBOSExecutor> executorSupplier,
      @NonNull Consumer<DBOSLifecycleListener> lifecycleConsumer) {
    this.config = Objects.requireNonNull(config);
    this.workflowRegistry = Objects.requireNonNull(workflowRegistry);
    this.executorSupplier = Objects.requireNonNull(executorSupplier);
    this.listenerConsumer = Objects.requireNonNull(lifecycleConsumer);
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
   * Returns the DBOS configuration supplied at construction time.
   *
   * @return the active {@link DBOSConfig}
   */
  public DBOSConfig config() {
    return this.config;
  }

  /**
   * Register a lifecycle listener that receives callbacks when DBOS is launched or shut down.
   *
   * @param listener the listener to register; must not be {@code null}
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
  public RegisteredWorkflow registerWorkflow(
      @NonNull Workflow wfTag,
      @NonNull Object target,
      @NonNull Method method,
      @Nullable String instanceName) {

    var workflowName = WorkflowRegistry.getWorkflowName(wfTag, method);
    var className = WorkflowRegistry.getWorkflowClassName(target);

    return registerWorkflow(
        workflowName,
        className,
        instanceName,
        target,
        method,
        wfTag.maxRecoveryAttempts(),
        wfTag.serializationStrategy());
  }

  /**
   * Register a workflow method with DBOS using explicit field values rather than deriving them from
   * a {@link Workflow} annotation. Prefer {@link #registerWorkflow(Workflow, Object, Method,
   * String)} unless you need to supply names or options that differ from the annotation.
   *
   * @param workflowName logical name of the workflow
   * @param className name of the class that declares the workflow method
   * @param instanceName optional instance name distinguishing multiple registrations of the same
   *     class; may be {@code null}
   * @param target the object instance on which the method will be invoked
   * @param method the workflow {@link Method}
   * @param maxRecoveryAttempts maximum number of recovery attempts; {@code null} uses the default
   * @param serializationStrategy strategy used to serialize and deserialize workflow arguments and
   *     return values; {@code null} uses the default
   * @throws IllegalStateException if called after DBOS is launched
   */
  public RegisteredWorkflow registerWorkflow(
      @NonNull String workflowName,
      @NonNull String className,
      @Nullable String instanceName,
      @NonNull Object target,
      @NonNull Method method,
      @Nullable Integer maxRecoveryAttempts,
      @Nullable SerializationStrategy serializationStrategy) {
    if (executorSupplier.get() != null) {
      throw new IllegalStateException("Cannot register workflow after DBOS is launched");
    }

    return workflowRegistry.registerWorkflow(
        workflowName,
        className,
        instanceName,
        target,
        method,
        maxRecoveryAttempts,
        serializationStrategy);
  }

  /**
   * Captured information about a workflow invocation: the workflow name, declaring class name,
   * optional instance name, and positional arguments.
   */
  public record CapturedInvocation(
      @NonNull String workflowName,
      @NonNull String className,
      @Nullable String instanceName,
      @NonNull Object[] args) {}

  /**
   * Capture the workflow invocation triggered by the supplied lambda without executing the
   * workflow. The lambda must call exactly one {@code @Workflow} method on a registered proxy. This
   * is intended for infrastructure that needs to defer a workflow start (for example, the
   * debouncer).
   *
   * @param wfLambda lambda that invokes exactly one workflow method on a registered proxy
   * @return the captured workflow name, class name, instance name, and arguments
   * @throws IllegalStateException if DBOS has not been launched
   */
  public <T, E extends Exception> CapturedInvocation captureInvocation(
      @NonNull ThrowingSupplier<T, E> wfLambda) {
    var invocation = executor("captureInvocation").captureInvocation(wfLambda);
    return new CapturedInvocation(
        invocation.workflowName(),
        invocation.className(),
        invocation.instanceName(),
        invocation.args());
  }

  /**
   * Find the workflow ID of the currently-active workflow with a given queue and deduplication ID.
   * Uses the unique {@code (queue_name, deduplication_id)} index for an O(1) point lookup. Returns
   * {@code null} if no active (ENQUEUED, DELAYED, or PENDING) workflow with that deduplication ID
   * exists in the given queue.
   *
   * @param queueName name of the queue to search
   * @param deduplicationId deduplication ID to look up
   * @return the workflow ID, or {@code null} if not found
   * @throws IllegalStateException if DBOS has not been launched
   */
  public @Nullable String findWorkflowIdByDeduplicationId(
      @NonNull String queueName, @NonNull String deduplicationId) {
    return executor("findWorkflowIdByDeduplicationId")
        .findWorkflowIdByDeduplicationId(queueName, deduplicationId);
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

  private static boolean isInternalWorkflow(RegisteredWorkflow wf) {
    return Constants.DEBOUNCER_WORKFLOW_NAME.equals(wf.workflowName());
  }

  private static boolean isInternalInstance(RegisteredWorkflowInstance inst) {
    return inst.target() instanceof DebouncerService;
  }

  /**
   * Get all user-registered workflows. Internal/system workflows registered by DBOS itself (for
   * example, the debouncer service workflow) are excluded.
   *
   * @return list of all user-registered workflow methods
   */
  public @NonNull Collection<RegisteredWorkflow> getRegisteredWorkflows() {
    var executor = executorSupplier.get();
    Collection<RegisteredWorkflow> all =
        executor != null
            ? executor.getRegisteredWorkflows()
            : workflowRegistry.getWorkflowSnapshot().values();
    return all.stream()
        .filter(wf -> !isInternalWorkflow(wf))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Get all user-registered workflow instances. Internal/system instances registered by DBOS itself
   * (for example, the debouncer service) are excluded.
   *
   * @return list of all user-registered class instances containing workflow methods
   */
  public @NonNull Collection<RegisteredWorkflowInstance> getRegisteredWorkflowInstances() {
    var executor = executorSupplier.get();
    Collection<RegisteredWorkflowInstance> all =
        executor != null
            ? executor.getRegisteredWorkflowInstances()
            : workflowRegistry.getInstanceSnapshot().values();
    return all.stream()
        .filter(inst -> !isInternalInstance(inst))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Finds a registered workflow by its workflow name and class name, using the default (empty)
   * instance name. Equivalent to calling {@link #getRegisteredWorkflow(String, String, String)}
   * with an empty string.
   *
   * @param workflowName the name of the workflow
   * @param className the name of the class containing the workflow
   * @return an {@link Optional} containing the {@link RegisteredWorkflow} if found, otherwise empty
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
      @NonNull String workflowName, @NonNull String className, @Nullable String instanceName) {
    var executor = executorSupplier.get();
    if (executor != null) {
      return executor.getRegisteredWorkflow(workflowName, className, instanceName);
    }
    var fqName = RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName);
    return Optional.ofNullable(workflowRegistry.getWorkflowSnapshot().get(fqName));
  }

  /**
   * Get a system database record stored by an external service. A unique value is stored per
   * combination of service, workflowName, and key.
   *
   * @param service identity of the service maintaining the record
   * @param workflowName fully qualified name of the workflow
   * @param key key assigned within the service+workflow scope
   * @return an {@link Optional} containing the value associated with the service+workflow+key
   *     combination, or empty if not found
   * @throws IllegalStateException if DBOS has not been launched
   */
  public Optional<ExternalState> getExternalState(String service, String workflowName, String key) {
    return executor("getExternalState").getExternalState(service, workflowName, key);
  }

  /**
   * Insert or update a system database record stored by an external service. A timestamped unique
   * value is stored per combination of service, workflowName, and key.
   *
   * @param state the {@link ExternalState} containing the service, workflow, key, and value to
   *     store
   * @return the value associated with the service+workflow+key combination — may differ from the
   *     supplied value if the existing record already had a higher version or timestamp
   * @throws IllegalStateException if DBOS has not been launched
   */
  public ExternalState upsertExternalState(ExternalState state) {
    return executor("upsertExternalState").upsertExternalState(state);
  }
}
