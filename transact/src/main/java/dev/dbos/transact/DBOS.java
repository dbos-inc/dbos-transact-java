package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSLifecycleListener;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.RegisteredWorkflowInstance;
import dev.dbos.transact.execution.ThrowingRunnable;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.internal.DBOSIntegration;
import dev.dbos.transact.internal.DBOSInvocationHandler;
import dev.dbos.transact.internal.QueueRegistry;
import dev.dbos.transact.internal.WorkflowRegistry;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.StepOptions;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowDelay;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Facade for context-based access to DBOS. `DBOS` is responsible for: Lifecycle - configuring,
 * launching, and shutting down DBOS Starting, enqueuing, and managing workflows Interacting with
 * workflows - getting status, results, events, and messages Accessing the workflow context Etc.
 */
public class DBOS implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DBOS.class);
  private static final String DBOS_VERSION = loadVersionFromResources();

  private final WorkflowRegistry workflowRegistry = new WorkflowRegistry();
  private final QueueRegistry queueRegistry = new QueueRegistry();
  private final Set<DBOSLifecycleListener> lifecycleRegistry = ConcurrentHashMap.newKeySet();
  private final DBOSConfig config;
  private final AtomicReference<DBOSExecutor> dbosExecutor = new AtomicReference<>();
  private final DBOSIntegration integration = new DBOSIntegration(dbosExecutor::get);

  private AlertHandler alertHandler;

  /**
   * Construct a new DBOS instance with the provided configuration.
   *
   * @param config the DBOS configuration; must not be null
   * @throws NullPointerException if config or required config fields are null
   */
  public DBOS(@NonNull DBOSConfig config) {
    Objects.requireNonNull(config, "DBOSConfig must not be null");
    Objects.requireNonNull(config.appName(), "DBOSConfig.appName must not be null");
    if (config.dataSource() == null) {
      Objects.requireNonNull(config.databaseUrl(), "DBOSConfig.databaseUrl must not be null");
      Objects.requireNonNull(config.dbUser(), "DBOSConfig.dbUser must not be null");
      Objects.requireNonNull(config.dbPassword(), "DBOSConfig.dbPassword must not be null");
    }

    this.config = config;
  }

  /**
   * Close this DBOS instance and shut down all associated resources. This method delegates to
   * {@link #shutdown()}.
   */
  @Override
  public void close() throws Exception {
    shutdown();
  }

  private static @Nullable String loadVersionFromResources() {
    final String PROPERTIES_FILE = "/dev/dbos/transact/app.properties";
    final String VERSION_KEY = "app.version";
    Properties props = new Properties();
    try (InputStream input = DBOS.class.getResourceAsStream(PROPERTIES_FILE)) {
      if (input == null) {
        logger.warn("Could not find {} resource file", PROPERTIES_FILE);
        return "<unknown (resource missing)>";
      }

      // Load the properties from the file
      props.load(input);

      // Retrieve the version property, defaulting to "unknown"
      return props.getProperty(VERSION_KEY, "<unknown>");
    } catch (IOException ex) {
      logger.error("Error loading version properties", ex);
      return "<unknown (IO Error)>";
    }
  }

  /**
   * Get the current DBOS version.
   *
   * @return the DBOS version string
   */
  public static String version() {
    return DBOS_VERSION;
  }

  /**
   * Register a lifecycle listener that receives callbacks when DBOS is launched or shut down
   *
   * @param listener
   */
  public void registerLifecycleListener(@NonNull DBOSLifecycleListener listener) {
    if (dbosExecutor.get() != null) {
      throw new IllegalStateException("Cannot register lifecycle listener after DBOS is launched");
    }

    lifecycleRegistry.add(listener);
  }

  /**
   * Register a DBOS queue. This must be called on each queue prior to launch, so that recovery has
   * the queue options available.
   *
   * @param queue `Queue` to register
   */
  public void registerQueue(@NonNull Queue queue) {
    if (dbosExecutor.get() != null) {
      throw new IllegalStateException("Cannot build a queue after DBOS is launched");
    }

    queueRegistry.register(queue);
  }

  /**
   * Register a set of DBOS queues. Each queue must be registered prior to launch, so that recovery
   * has the queue options available.
   *
   * @param queues collection of `Queue` instances to register
   */
  public void registerQueues(@NonNull Queue... queues) {
    for (Queue queue : queues) {
      registerQueue(queue);
    }
  }

  /**
   * Register all workflows and steps in the provided class instance
   *
   * @param <T> The interface type for the instance
   * @param interfaceClass The interface class for the workflows
   * @param target An implementation instance providing the workflow and step function code
   * @return A proxy, with interface {@literal <T>}, that provides durability for the workflow
   *     functions
   */
  public <T> @NonNull T registerProxy(@NonNull Class<T> interfaceClass, @NonNull T target) {
    return registerProxy(interfaceClass, target, null);
  }

  /**
   * Register all workflows and steps in the provided class instance
   *
   * @param <T> The interface type for the instance
   * @param interfaceClass The interface class for the workflows
   * @param target An implementation instance providing the workflow and step function code
   * @param instanceName Name of the instance, allowing multiple instances of the same class to be
   *     registered
   * @return A proxy, with interface {@literal <T>}, that provides durability for the workflow
   *     functions
   */
  public <T> @NonNull T registerProxy(
      @NonNull Class<T> interfaceClass, @NonNull T target, @Nullable String instanceName) {
    if (dbosExecutor.get() != null) {
      throw new IllegalStateException("Cannot register workflow after DBOS is launched");
    }
    Objects.requireNonNull(interfaceClass, "interfaceClass must not be null");
    Objects.requireNonNull(target, "target must not be null");

    if (!hasWorkflowsOrSteps(target)) {
      throw new IllegalArgumentException("Target does not contain any @Workflow or @Step methods");
    }

    workflowRegistry.registerInstance(instanceName, target);

    for (var method : target.getClass().getDeclaredMethods()) {
      var wfTag = method.getAnnotation(Workflow.class);
      if (wfTag != null) {
        method.setAccessible(true); // In case it's not public
        workflowRegistry.registerWorkflow(wfTag, target, method, instanceName);
      }
    }

    return DBOSInvocationHandler.createProxy(
        interfaceClass, target, instanceName, dbosExecutor::get);
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
    if (dbosExecutor.get() != null) {
      throw new IllegalStateException("Cannot register workflow after DBOS is launched");
    }

    workflowRegistry.registerWorkflow(wfTag, target, method, instanceName);
  }

  /**
   * Check if the provided target object contains any methods annotated with @Workflow.
   *
   * @param target the object to check for workflow methods
   * @return true if the target contains at least one @Workflow annotated method, false otherwise
   * @throws NullPointerException if target is null
   */
  static boolean hasWorkflowsOrSteps(@NonNull Object target) {
    var methods =
        Objects.requireNonNull(target, "target can not be null").getClass().getDeclaredMethods();
    for (var method : methods) {
      if (method.isAnnotationPresent(Workflow.class) || method.isAnnotationPresent(Step.class)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Registers an {@link AlertHandler} to handle alerts generated by DBOS. This method must be
   * called before DBOS is launched; attempting to register an alert handler after launch will
   * result in an {@link IllegalStateException}.
   *
   * @param handler the {@link AlertHandler} instance to register; must not be null
   * @throws IllegalStateException if called after DBOS has been launched
   */
  public void registerAlertHandler(AlertHandler handler) {
    if (dbosExecutor.get() != null) {
      throw new IllegalStateException("Cannot set alert handler after DBOS is launched");
    }

    this.alertHandler = handler;
  }

  // package private method for test purposes
  @Nullable DBOSExecutor getDbosExecutor() {
    return dbosExecutor.get();
  }

  /**
   * Launch DBOS, and start recovery. All workflows, queues, and other objects should be registered
   * before launch
   */
  public void launch() {
    logger.info("Launching DBOS v{}", DBOS.version());

    if (dbosExecutor.get() == null) {
      var executor = new DBOSExecutor(config);

      if (dbosExecutor.compareAndSet(null, executor)) {
        if (config.migrate()) {
          MigrationManager.runMigrations(config);
        }

        executor.start(
            this,
            new HashSet<>(this.lifecycleRegistry),
            workflowRegistry.getWorkflowSnapshot(),
            workflowRegistry.getInstanceSnapshot(),
            queueRegistry.getSnapshot(),
            alertHandler);
      }
    }
  }

  /**
   * Shut down DBOS. This method should only be used in test environments, where DBOS is used
   * multiple times in the same JVM.
   */
  public void shutdown() {
    var current = dbosExecutor.get();
    if (current != null) {
      current.close();
      if (!dbosExecutor.compareAndSet(current, null)) {
        logger.error("failed to set DBOS executor to null on shut down");
      }
    }
    logger.info("DBOS shut down");
  }

  // helper for methods that can only be called after launch
  private DBOSExecutor ensureLaunched(String caller) {
    var exec = dbosExecutor.get();
    if (exec == null) {
      throw new IllegalStateException(
          String.format("Cannot call %s before DBOS is launched", caller));
    }
    return exec;
  }

  /**
   * Retrieve a queue definition
   *
   * @param queueName Name of the queue
   * @return Optional containing the queue definition for given `queueName`, or empty if not found
   */
  public @NonNull Optional<Queue> getQueue(@NonNull String queueName) {
    return ensureLaunched("getQueue").getQueue(queueName);
  }

  /**
   * Durable sleep. Use this instead of Thread.sleep, especially in workflows. On restart or during
   * recovery the original expected wakeup time is honoured as opposed to sleeping all over again.
   *
   * @param duration amount of time to sleep
   */
  public void sleep(@NonNull Duration duration) {
    if (!DBOSContext.inWorkflow()) {
      try {
        Thread.sleep(duration.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else if (DBOSContext.inStep()) {
      try {
        Thread.sleep(duration.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      ensureLaunched("sleep").sleep(duration);
    }
  }

  /**
   * Start or enqueue a workflow with a return value
   *
   * @param <T> Return type of the workflow
   * @param <E> Type of checked exception thrown by the workflow, if any
   * @param supplier A lambda that calls exactly one workflow function
   * @param options Start workflow options
   * @return A handle to the enqueued or running workflow
   */
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> startWorkflow(
      @NonNull ThrowingSupplier<T, E> supplier, @Nullable StartWorkflowOptions options) {
    return ensureLaunched("startWorkflow").startWorkflow(supplier, options);
  }

  /**
   * Start or enqueue a workflow with default options
   *
   * @param <T> Return type of the workflow
   * @param <E> Type of checked exception thrown by the workflow, if any
   * @param supplier A lambda that calls exactly one workflow function
   * @return A handle to the enqueued or running workflow
   */
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> startWorkflow(
      @NonNull ThrowingSupplier<T, E> supplier) {
    return startWorkflow(supplier, null);
  }

  /**
   * Start or enqueue a workflow with no return value
   *
   * @param <E> Type of checked exception thrown by the workflow, if any
   * @param runnable A lambda that calls exactly one workflow function
   * @param options Start workflow options
   * @return A handle to the enqueued or running workflow
   */
  public <E extends Exception> @NonNull WorkflowHandle<Void, E> startWorkflow(
      @NonNull ThrowingRunnable<E> runnable, @Nullable StartWorkflowOptions options) {
    return startWorkflow(
        () -> {
          runnable.execute();
          return null;
        },
        options);
  }

  /**
   * Start or enqueue a workflow with no return value, using default options
   *
   * @param <E> Type of checked exception thrown by the workflow, if any
   * @param runnable A lambda that calls exactly one workflow function
   * @return A handle to the enqueued or running workflow
   */
  public <E extends Exception> @NonNull WorkflowHandle<Void, E> startWorkflow(
      @NonNull ThrowingRunnable<E> runnable) {
    return startWorkflow(runnable, null);
  }

  /**
   * Returns the DBOS integration APIs for use by specialized integrations such as AOP aspects and
   * event listeners.
   *
   * <p>The returned {@link DBOSIntegration} instance is <strong>not part of the primary public
   * API</strong> and may change without notice. Application code should use the methods on this
   * class directly instead.
   *
   * @return the {@link DBOSIntegration} accessor for this DBOS instance
   */
  public @NonNull DBOSIntegration integration() {
    return integration;
  }

  /**
   * Get the result of a workflow, or rethrow the exception thrown by the workflow
   *
   * @param <T> Return type of the workflow
   * @param <E> Checked exception type, if any, thrown by the workflow
   * @param workflowId ID of the workflow to retrieve
   * @return Return value of the workflow
   * @throws E if the workflow threw an exception
   */
  public <T, E extends Exception> T getResult(@NonNull String workflowId) throws E {
    return ensureLaunched("getResult").<T, E>getResult(workflowId);
  }

  /**
   * Get the status of a workflow
   *
   * @param workflowId ID of the workflow to query
   * @return Current workflow status for the provided workflowId, or empty if no such workflow
   *     exists.
   */
  public @NonNull Optional<WorkflowStatus> getWorkflowStatus(@NonNull String workflowId) {
    return Optional.ofNullable(ensureLaunched("getWorkflowStatus").getWorkflowStatus(workflowId));
  }

  /**
   * Send a message to a workflow
   *
   * @param destinationId recipient of the message
   * @param message message to be sent
   * @param topic topic to which the message is send
   * @param idempotencyKey optional idempotency key for exactly-once send
   */
  public void send(
      @NonNull String destinationId,
      @NonNull Object message,
      @Nullable String topic,
      @Nullable String idempotencyKey) {
    send(destinationId, message, topic, idempotencyKey, null);
  }

  /**
   * Send a message to a workflow
   *
   * @param destinationId recipient of the message
   * @param message message to be sent
   * @param topic topic to which the message is send
   */
  public void send(@NonNull String destinationId, @NonNull Object message, @Nullable String topic) {
    send(destinationId, message, topic, null, null);
  }

  /**
   * Send a message to a workflow with serialization strategy
   *
   * @param destinationId recipient of the message
   * @param message message to be sent
   * @param topic topic to which the message is send
   * @param idempotencyKey optional idempotency key for exactly-once send
   * @param serialization serialization strategy to use (null for default)
   */
  public void send(
      @NonNull String destinationId,
      @NonNull Object message,
      @Nullable String topic,
      @Nullable String idempotencyKey,
      @Nullable SerializationStrategy serialization) {
    if (serialization == null) serialization = SerializationStrategy.DEFAULT;
    ensureLaunched("send").send(destinationId, message, topic, idempotencyKey, serialization);
  }

  /**
   * Get a message sent to a particular topic
   *
   * @param topic the topic whose message to get
   * @param timeout duration after which the call times out
   * @return the message if there is one or else null
   */
  public @NonNull @SuppressWarnings("unchecked") <T> Optional<T> recv(
      @Nullable String topic, @NonNull Duration timeout) {
    return Optional.ofNullable((T) ensureLaunched("recv").recv(topic, timeout));
  }

  /**
   * Call within a workflow to publish a key value pair. Uses the workflow's serialization format.
   *
   * @param key identifier for published data
   * @param value data that is published
   */
  public void setEvent(@NonNull String key, @NonNull Object value) {
    setEvent(key, value, null);
  }

  /**
   * Call within a workflow to publish a key value pair with a specific serialization strategy.
   *
   * @param key identifier for published data
   * @param value data that is published
   * @param serialization serialization strategy to use (null to use workflow's default)
   */
  public void setEvent(
      @NonNull String key, @NonNull Object value, @Nullable SerializationStrategy serialization) {
    // If no explicit serialization specified, use the workflow context's serialization
    if (serialization == null) {
      serialization = serializationStrategy();
    }
    ensureLaunched("setEvent").setEvent(key, value, serialization);
  }

  /**
   * Get the data published by a workflow
   *
   * @param workflowId id of the workflow who data is to be retrieved
   * @param key identifies the data
   * @param timeout time to wait for data before timing out
   * @return Optional containing the published value if available, or empty if timeout occurs or no
   *     value found
   */
  public @NonNull @SuppressWarnings("unchecked") <T> Optional<T> getEvent(
      @NonNull String workflowId, @NonNull String key, @NonNull Duration timeout) {
    logger.debug("Received getEvent for {} {}", workflowId, key);

    return Optional.ofNullable((T) ensureLaunched("getEvent").getEvent(workflowId, key, timeout));
  }

  /**
   * Run the provided function as a step; this variant is for functions with a return value
   *
   * @param <E> Checked exception thrown by the step, if any
   * @param stepfunc function or lambda to run
   * @param opts step name, and retry options for running the step
   * @throws E
   */
  public <T, E extends Exception> T runStep(
      @NonNull ThrowingSupplier<T, E> stepfunc, @NonNull StepOptions opts) throws E {

    return ensureLaunched("runStep").runStep(stepfunc, opts, null);
  }

  /**
   * Run the provided function as a step; this variant is for functions with a return value
   *
   * @param <E> Checked exception thrown by the step, if any
   * @param stepfunc function or lambda to run
   * @param name name of the step, for tracing and to record in the system database
   * @throws E
   */
  public <T, E extends Exception> T runStep(
      @NonNull ThrowingSupplier<T, E> stepfunc, @NonNull String name) throws E {
    return runStep(stepfunc, new StepOptions(name));
  }

  /**
   * Run the provided function as a step; this variant is for functions with no return value
   *
   * @param <E> Checked exception thrown by the step, if any
   * @param stepfunc function or lambda to run
   * @param opts step name, and retry options for running the step
   * @throws E
   */
  public <E extends Exception> void runStep(
      @NonNull ThrowingRunnable<E> stepfunc, @NonNull StepOptions opts) throws E {
    runStep(
        () -> {
          stepfunc.execute();
          return null;
        },
        opts);
  }

  /**
   * Run the provided function as a step; this variant is for functions with no return value
   *
   * @param <E> Checked exception thrown by the step, if any
   * @param stepfunc function or lambda to run
   * @param name Name of the step, for tracing and recording in the system database
   * @throws E
   */
  public <E extends Exception> void runStep(
      @NonNull ThrowingRunnable<E> stepfunc, @NonNull String name) throws E {
    runStep(stepfunc, new StepOptions(name));
  }

  /**
   * Resume a workflow starting from the step after the last complete step. This method allows
   * resuming workflows that were previously interrupted, failed, or canceled. The workflow will
   * continue execution from where it left off, replaying any completed steps deterministically.
   *
   * @param <T> Return type of the workflow function
   * @param <E> Type of checked exception thrown by the workflow function, if any
   * @param workflowId ID of the workflow to resume; must not be null
   * @param queueName optional queue name to enqueue the resumed workflow to; if null, the workflow
   *     will be resumed in the default execution context
   * @return A handle to the resumed workflow
   * @throws IllegalStateException if called before DBOS is launched
   */
  @SuppressWarnings("unchecked")
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> resumeWorkflow(
      @NonNull String workflowId, @Nullable String queueName) {
    var handles = resumeWorkflows(List.of(workflowId), queueName);
    assert (handles.size() == 1);
    return (WorkflowHandle<T, E>) handles.get(0);
  }

  /**
   * Resume a workflow starting from the step after the last complete step using the default queue.
   * This method is equivalent to calling {@code resumeWorkflow(workflowId, null)}. The workflow
   * will continue execution from where it left off, replaying any completed steps
   * deterministically.
   *
   * @param <T> Return type of the workflow function
   * @param <E> Type of checked exception thrown by the workflow function, if any
   * @param workflowId ID of the workflow to resume; must not be null
   * @return A handle to the resumed workflow
   * @throws IllegalStateException if called before DBOS is launched
   */
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> resumeWorkflow(
      @NonNull String workflowId) {
    return resumeWorkflow(workflowId, null);
  }

  /**
   * Resume multiple workflows starting from the step after the last complete step for each
   * workflow. This method allows bulk resumption of workflows that were previously interrupted,
   * failed, or canceled. Each workflow will continue execution from where it left off, replaying
   * any completed steps deterministically.
   *
   * @param workflowIds a list of workflow IDs to resume; must not be null
   * @param queueName optional queue name to enqueue the resumed workflows to; if null, the
   *     workflows will be resumed in the default execution context
   * @return A list of handles to the resumed workflows
   * @throws IllegalStateException if called before DBOS is launched
   */
  public @NonNull List<WorkflowHandle<Object, Exception>> resumeWorkflows(
      @NonNull List<String> workflowIds, @Nullable String queueName) {
    return ensureLaunched("resumeWorkflow").resumeWorkflows(workflowIds, queueName);
  }

  /**
   * Resume multiple workflows starting from the step after the last complete step for each workflow
   * using the default queue. This method is equivalent to calling {@code
   * resumeWorkflows(workflowIds, null)}. Each workflow will continue execution from where it left
   * off, replaying any completed steps deterministically.
   *
   * @param workflowIds a list of workflow IDs to resume; must not be null
   * @return A list of handles to the resumed workflows
   * @throws IllegalStateException if called before DBOS is launched
   */
  public @NonNull List<WorkflowHandle<Object, Exception>> resumeWorkflows(
      @NonNull List<String> workflowIds) {
    return resumeWorkflows(workflowIds, null);
  }

  /***
   *
   * Cancel the workflow. After this function is called, the next step (not the
   * current one) will not execute
   *
   * @param workflowId ID of the workflow to cancel
   * @throws IllegalStateException if called before DBOS is launched
   */
  public void cancelWorkflow(@NonNull String workflowId) {
    cancelWorkflows(List.of(workflowId));
  }

  /**
   * Cancels multiple workflows. After this function is called, the next step (not the current one)
   * of each specified workflow will not execute.
   *
   * @param workflowIds a list of workflow IDs to cancel; must not be null
   * @throws IllegalStateException if called before DBOS is launched
   */
  public void cancelWorkflows(@NonNull List<String> workflowIds) {
    ensureLaunched("cancelWorkflow").cancelWorkflows(workflowIds);
  }

  /**
   * Delete a workflow from the system. This permanently removes the workflow and its associated
   * data from the database. Child workflows are preserved by default.
   *
   * @param workflowId ID of the workflow to delete; must not be null
   * @throws IllegalStateException if called before DBOS is launched
   */
  public void deleteWorkflow(@NonNull String workflowId) {
    deleteWorkflows(List.of(workflowId), false);
  }

  /**
   * Delete a workflow from the system. This permanently removes the workflow and its associated
   * data from the database.
   *
   * @param workflowId ID of the workflow to delete; must not be null
   * @param deleteChildren if true, also delete any child workflows; if false, preserve child
   *     workflows
   * @throws IllegalStateException if called before DBOS is launched
   */
  public void deleteWorkflow(@NonNull String workflowId, boolean deleteChildren) {
    deleteWorkflows(List.of(workflowId), deleteChildren);
  }

  /**
   * Delete multiple workflows from the system. This permanently removes the workflows and their
   * associated data from the database. Child workflows are preserved by default.
   *
   * @param workflowIds a list of workflow IDs to delete; must not be null
   * @throws IllegalStateException if called before DBOS is launched
   */
  public void deleteWorkflows(@NonNull List<String> workflowIds) {
    deleteWorkflows(workflowIds, false);
  }

  /**
   * Delete multiple workflows from the system. This permanently removes the workflows and their
   * associated data from the database.
   *
   * @param workflowIds a list of workflow IDs to delete; must not be null
   * @param deleteChildren if true, also delete any child workflows; if false, preserve child
   *     workflows
   * @throws IllegalStateException if called before DBOS is launched
   */
  public void deleteWorkflows(@NonNull List<String> workflowIds, boolean deleteChildren) {
    ensureLaunched("deleteWorkflows").deleteWorkflows(workflowIds, deleteChildren);
  }

  /**
   * Fork the workflow. Re-execute with another Id from the step provided. Steps prior to the
   * provided step are copied over
   *
   * @param <T> Return type of the workflow function
   * @param <E> Checked exception thrown by the workflow function, if any
   * @param workflowId Original workflow Id
   * @param startStep Start execution from this step. Prior steps copied over
   * @param options {@link ForkOptions} containing forkedWorkflowId, applicationVersion, timeout
   * @return handle to the workflow
   */
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> forkWorkflow(
      @NonNull String workflowId, int startStep, @NonNull ForkOptions options) {
    return ensureLaunched("forkWorkflow").forkWorkflow(workflowId, startStep, options);
  }

  /**
   * Fork the workflow. Re-execute with another Id from the step provided. Steps prior to the
   * provided step are copied over
   *
   * @param <T> Return type of the workflow function
   * @param <E> Checked exception thrown by the workflow function, if any
   * @param workflowId Original workflow Id
   * @param startStep Start execution from this step. Prior steps copied over
   * @return handle to the workflow
   */
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> forkWorkflow(
      @NonNull String workflowId, int startStep) {
    return forkWorkflow(workflowId, startStep, new ForkOptions());
  }

  /**
   * List all registered application versions, ordered by timestamp descending.
   *
   * @return list of {@link VersionInfo} records
   */
  public @NonNull List<VersionInfo> listApplicationVersions() {
    return ensureLaunched("listApplicationVersions").listApplicationVersions();
  }

  /**
   * Get the most recently promoted application version.
   *
   * @return the latest {@link VersionInfo}
   */
  public @NonNull VersionInfo getLatestApplicationVersion() {
    return ensureLaunched("getLatestApplicationVersion").getLatestApplicationVersion();
  }

  /**
   * Promote a version to be the latest application version.
   *
   * @param versionName the version to promote
   */
  public void setLatestApplicationVersion(@NonNull String versionName) {
    ensureLaunched("setLatestApplicationVersion").setLatestApplicationVersion(versionName);
  }

  /**
   * Create a cron schedule that periodically invokes a workflow. The scheduleId is generated if
   * null.
   *
   * @param schedule the schedule configuration
   */
  public void createSchedule(@NonNull WorkflowSchedule schedule) {
    ensureLaunched("createSchedule").createSchedule(schedule);
  }

  /**
   * Get a schedule by name.
   *
   * @param name schedule name
   * @return the schedule, or empty if not found
   */
  public @NonNull Optional<WorkflowSchedule> getSchedule(@NonNull String name) {
    return ensureLaunched("getSchedule").getSchedule(name);
  }

  /**
   * List schedules with optional filters.
   *
   * @param status filter by status (e.g. "ACTIVE", "PAUSED"); null means no filter
   * @param workflowName filter by workflow name; null means no filter
   * @param namePrefix filter by schedule name prefix; null means no filter
   * @return matching schedules
   */
  public @NonNull List<WorkflowSchedule> listSchedules(
      @Nullable List<ScheduleStatus> status,
      @Nullable List<String> workflowName,
      @Nullable List<String> namePrefix) {
    return ensureLaunched("listSchedules").listSchedules(status, workflowName, namePrefix);
  }

  /**
   * Delete a schedule by name. No-op if the schedule does not exist.
   *
   * @param name schedule name
   */
  public void deleteSchedule(@NonNull String name) {
    ensureLaunched("deleteSchedule").deleteSchedule(name);
  }

  /**
   * Pause a schedule. A paused schedule does not fire.
   *
   * @param name schedule name
   */
  public void pauseSchedule(@NonNull String name) {
    ensureLaunched("pauseSchedule").pauseSchedule(name);
  }

  /**
   * Resume a paused schedule so it begins firing again.
   *
   * @param name schedule name
   */
  public void resumeSchedule(@NonNull String name) {
    ensureLaunched("resumeSchedule").resumeSchedule(name);
  }

  /**
   * Atomically create or replace a set of schedules. Each schedule is deleted (if it exists) and
   * re-created in a single transaction.
   *
   * @param schedules the schedules to apply
   */
  public void applySchedules(@NonNull List<WorkflowSchedule> schedules) {
    ensureLaunched("applySchedules").applySchedules(schedules);
  }

  // /**
  //  * Enqueue all executions of a schedule that would have run between {@code start} (exclusive)
  // and
  //  * {@code end} (exclusive). Uses the same deterministic workflow IDs as the live scheduler, so
  //  * already-executed times are skipped.
  //  *
  //  * @param scheduleName name of an existing schedule
  //  * @param start start of the backfill window (exclusive)
  //  * @param end end of the backfill window (exclusive)
  //  * @return handles to the enqueued executions
  //  */
  public @NonNull List<WorkflowHandle<Object, Exception>> backfillSchedule(
      @NonNull String scheduleName, @NonNull Instant start, @NonNull Instant end) {
    return ensureLaunched("backfillSchedule").backfillSchedule(scheduleName, start, end);
  }

  // /**
  //  * Immediately enqueue the scheduled workflow at the current time.
  //  *
  //  * @param scheduleName name of an existing schedule
  //  * @return handle to the enqueued execution
  //  */
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> triggerSchedule(
      @NonNull String scheduleName) {
    return ensureLaunched("triggerSchedule").triggerSchedule(scheduleName);
  }

  /**
   * Retrieve a handle to a workflow, given its ID. Note that a handle is always returned, whether
   * the workflow exists or not; getStatus() can be used to tell the difference
   *
   * @param <T> Return type of the workflow function
   * @param <E> Checked exception thrown by the workflow function, if any
   * @param workflowId ID of the workflow to retrieve
   * @return Workflow handle for the provided workflow ID
   */
  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> retrieveWorkflow(
      @NonNull String workflowId) {
    return ensureLaunched("retrieveWorkflow").retrieveWorkflow(workflowId);
  }

  /**
   * Sets a delay for a workflow, causing it to be paused for a specified duration or until a
   * specific time. This is useful for implementing delays, timeouts, or scheduling workflows to
   * resume at a later time.
   *
   * @param workflowId the unique identifier of the workflow to delay
   * @param delay the duration to delay the workflow from now
   * @throws IllegalArgumentException if the workflow ID is invalid
   * @throws IllegalStateException if DBOS has not been launched
   */
  public void setWorkflowDelay(@NonNull String workflowId, @NonNull Duration delay) {
    var wfDelay = new WorkflowDelay.Delay(Objects.requireNonNull(delay, "delay must not be null"));
    ensureLaunched("setWorkflowDelay").setWorkflowDelay(workflowId, wfDelay);
  }

  /**
   * Sets a delay for a workflow, causing it to be paused for a specified duration or until a
   * specific time. This is useful for implementing delays, timeouts, or scheduling workflows to
   * resume at a later time.
   *
   * @param workflowId the unique identifier of the workflow to delay
   * @param delayUntil the absolute time until which to delay the workflow
   * @throws IllegalArgumentException if the workflow ID is invalid
   * @throws IllegalStateException if DBOS has not been launched
   */
  public void setWorkflowDelay(@NonNull String workflowId, @NonNull Instant delayUntil) {
    var wfDelay =
        new WorkflowDelay.DelayUntil(
            Objects.requireNonNull(delayUntil, "delayUntil must not be null"));
    ensureLaunched("setWorkflowDelay").setWorkflowDelay(workflowId, wfDelay);
  }

  /**
   * Retrieves all events stored during the execution of a workflow. Events are key-value pairs that
   * workflows can set during execution to persist intermediate state or communicate between steps.
   * This method returns all events for the specified workflow with their deserialized values.
   *
   * @param workflowId the unique identifier of the workflow whose events to retrieve
   * @return a map containing all events for the workflow, where keys are event names and values are
   *     the deserialized event data
   */
  public @NonNull Map<String, Object> getAllEvents(@NonNull String workflowId) {
    return ensureLaunched("getAllEvents").getAllEvents(workflowId);
  }

  /**
   * List workflows matching the supplied input filter criteria
   *
   * @param input {@link ListWorkflowsInput} parameters to query workflows. Pass null to list all
   *     workflows.
   * @return a list of workflow status {@link WorkflowStatus}
   */
  public @NonNull List<WorkflowStatus> listWorkflows(@Nullable ListWorkflowsInput input) {
    return ensureLaunched("listWorkflows").listWorkflows(input);
  }

  /**
   * List the steps in the workflow
   *
   * @param workflowId Id of the workflow whose steps to return
   * @return list of step information {@link StepInfo}
   */
  public @NonNull List<StepInfo> listWorkflowSteps(@NonNull String workflowId) {
    return listWorkflowSteps(workflowId, null, null);
  }

  /**
   * List the steps in the workflow with optional pagination
   *
   * @param workflowId Id of the workflow whose steps to return
   * @param limit Maximum number of steps to return
   * @param offset Number of steps to skip before returning
   * @return list of step information {@link StepInfo}
   */
  public @NonNull List<StepInfo> listWorkflowSteps(
      @NonNull String workflowId, Integer limit, Integer offset) {
    return ensureLaunched("listWorkflowSteps").listWorkflowSteps(workflowId, true, limit, offset);
  }

  /**
   * Get all workflows registered with DBOS.
   *
   * @return list of all registered workflow methods
   */
  public @NonNull Collection<RegisteredWorkflow> getRegisteredWorkflows() {
    return ensureLaunched("getRegisteredWorkflows").getRegisteredWorkflows();
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
    return ensureLaunched("getRegisteredWorkflow")
        .getRegisteredWorkflow(workflowName, className, instanceName);
  }

  /**
   * Get all workflow classes registered with DBOS.
   *
   * @return list of all class instances containing registered workflow methods
   */
  public @NonNull Collection<RegisteredWorkflowInstance> getRegisteredWorkflowInstances() {
    return ensureLaunched("getRegisteredWorkflowInstances").getRegisteredWorkflowInstances();
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
    return ensureLaunched("getExternalState").getExternalState(service, workflowName, key);
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
    return ensureLaunched("upsertExternalState").upsertExternalState(state);
  }

  /**
   * Marks a breaking change within a workflow. Returns true for new workflows (i.e. workflow sthat
   * reach this point in the workflow after the breaking change was created) and false for old
   * worklows (i.e. workflows that reached this point in the workflow before the breaking change was
   * created). The workflow should execute the new code if this method returns true, otherwise
   * execute the old code. Note, patching must be enabled in DBOS configuration and this method must
   * be called from within a workflow context.
   *
   * @param patchName the name of the patch to apply
   * @return true for workflows started after the breaking change, false for workflows started
   *     before the breaking change
   * @throws RuntimeException if patching is not enabled in DBOS config or if called outside a
   *     workflow
   */
  public boolean patch(@NonNull String patchName) {
    return ensureLaunched("patch").patch(patchName);
  }

  /**
   * Deprecates a previously applied breaking change patch within a workflow. Safely executes
   * workflows containing the patch marker, but does not insert the patch marker into new workflows.
   * Always returns true (boolean return gives deprecatePatch the same signature as {@link #patch}).
   * Like {@link #patch}, patching must be enabled in DBOS configuration and this method must be
   * called from within a workflow context.
   *
   * @param patchName the name of the patch to deprecate
   * @return true (always returns true or throws)
   * @throws RuntimeException if patching is not enabled in DBOS config or if called outside a
   *     workflow
   */
  public boolean deprecatePatch(@NonNull String patchName) {
    return ensureLaunched("deprecatePatch").deprecatePatch(patchName);
  }

  /**
   * Get the ID of the current running workflow, or `null` if a workflow is not in progress
   *
   * @return Current workflow ID
   */
  public static @Nullable String workflowId() {
    return DBOSContext.workflowId();
  }

  /**
   * Get the ID of the current running step, or `null` if a workflow step is not in progress
   *
   * @return Current step ID number
   */
  public static @Nullable Integer stepId() {
    return DBOSContext.stepId();
  }

  /**
   * @return `true` if the current calling context is executing a workflow, or false otherwise
   */
  public static boolean inWorkflow() {
    return DBOSContext.inWorkflow();
  }

  /**
   * @return `true` if the current calling context is executing a workflow step, or false otherwise
   */
  public static boolean inStep() {
    return DBOSContext.inStep();
  }

  /**
   * Get the serialization format of the current workflow context.
   *
   * @return the SerializationStrategy (e.g., "portable_json", "java_jackson"), or null if not in a
   *     workflow context or using default serialization
   */
  public static @Nullable SerializationStrategy serializationStrategy() {
    return DBOSContext.serializationStrategy();
  }

  /**
   * Write a value to a stream. Must be called from within a workflow or step.
   *
   * @param key The stream key / name within the workflow
   * @param value A serializable value to write to the stream
   */
  public void writeStream(@NonNull String key, @NonNull Object value) {
    writeStream(key, value, null);
  }

  /**
   * Write a value to a stream with a specific serialization strategy. Must be called from within a
   * workflow or step.
   *
   * @param key The stream key / name within the workflow
   * @param value A serializable value to write to the stream
   * @param serialization The serialization strategy to use (null for workflow default)
   */
  public void writeStream(
      @NonNull String key, @NonNull Object value, @Nullable SerializationStrategy serialization) {
    ensureLaunched("writeStream").writeStream(key, value, serialization);
  }

  /**
   * Close a stream. Must be called from within a workflow, not a step.
   *
   * @param key The stream key / name within the workflow
   */
  public void closeStream(@NonNull String key) {
    ensureLaunched("closeStream").closeStream(key);
  }

  /**
   * Read values from a stream as an iterator. This function reads values from a stream identified
   * by the workflow_id and key, returning an iterator that yields each value in order until the
   * stream is closed or the workflow terminates.
   *
   * @param workflowId The workflow instance ID that owns the stream
   * @param key The stream key / name within the workflow
   * @return Iterator that yields each value in the stream
   */
  public @NonNull Iterator<Object> readStream(@NonNull String workflowId, @NonNull String key) {
    return ensureLaunched("readStream").readStream(workflowId, key);
  }
}
