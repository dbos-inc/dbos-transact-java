package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSLifecycleListener;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.RegisteredWorkflowInstance;
import dev.dbos.transact.execution.ThrowingRunnable;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.internal.DBOSInvocationHandler;
import dev.dbos.transact.internal.QueueRegistry;
import dev.dbos.transact.internal.WorkflowRegistry;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.tempworkflows.InternalWorkflowsService;
import dev.dbos.transact.tempworkflows.InternalWorkflowsServiceImpl;
import dev.dbos.transact.workflow.*;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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
public class DBOS {
  private DBOS() {}

  private static final Logger logger = LoggerFactory.getLogger(DBOS.class);
  private static final String version = loadVersionFromResources();

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

  public static @Nullable String version() {
    return version;
  }

  public static class Instance {
    private final WorkflowRegistry workflowRegistry = new WorkflowRegistry();
    private final QueueRegistry queueRegistry = new QueueRegistry();
    private final Set<DBOSLifecycleListener> lifecycleRegistry = ConcurrentHashMap.newKeySet();

    private DBOSConfig config;

    private InternalWorkflowsService internalWorkflowsService;

    private final AtomicReference<DBOSExecutor> dbosExecutor = new AtomicReference<>();

    private Instance() {
      DBOSContextHolder.clear(); // CB: Why
    }

    private void registerClassWorkflows(
        @NonNull Class<?> interfaceClass,
        @NonNull Object implementation,
        @Nullable String instanceName) {
      Objects.requireNonNull(interfaceClass, "interfaceClass must not be null");
      Objects.requireNonNull(implementation, "implementation must not be null");
      instanceName = Objects.requireNonNullElse(instanceName, "");
      if (!interfaceClass.isInterface()) {
        throw new IllegalArgumentException("interfaceClass must be an interface");
      }
      if (dbosExecutor.get() != null) {
        throw new IllegalStateException("Cannot register workflow after DBOS is launched");
      }

      // Use @WorkflowClassName annotation if present, otherwise use the Java class name
      WorkflowClassName classNameAnnotation =
          implementation.getClass().getAnnotation(WorkflowClassName.class);
      String className =
          (classNameAnnotation != null && !classNameAnnotation.value().isEmpty())
              ? classNameAnnotation.value()
              : implementation.getClass().getName();
      workflowRegistry.register(interfaceClass, implementation, className, instanceName);

      Method[] methods = implementation.getClass().getDeclaredMethods();
      for (Method method : methods) {
        Workflow wfAnnotation = method.getAnnotation(Workflow.class);
        if (wfAnnotation != null) {
          method.setAccessible(true); // In case it's not public
          registerWorkflowMethod(wfAnnotation, implementation, className, instanceName, method);
        }
      }
    }

    private @NonNull String registerWorkflowMethod(
        @NonNull Workflow wfTag,
        @NonNull Object target,
        @NonNull String className,
        @NonNull String instanceName,
        @NonNull Method method) {
      if (dbosExecutor.get() != null) {
        throw new IllegalStateException("Cannot register workflow after DBOS is launched");
      }

      String name = wfTag.name().isEmpty() ? method.getName() : wfTag.name();
      workflowRegistry.register(
          className, name, target, instanceName, method, wfTag.maxRecoveryAttempts());
      return name;
    }

    void registerLifecycleListener(@NonNull DBOSLifecycleListener l) {
      if (dbosExecutor.get() != null) {
        throw new IllegalStateException(
            "Cannot register lifecycle listener after DBOS is launched");
      }

      lifecycleRegistry.add(l);
    }

    void registerQueue(@NonNull Queue queue) {
      if (dbosExecutor.get() != null) {
        throw new IllegalStateException("Cannot build a queue after DBOS is launched");
      }

      queueRegistry.register(queue);
    }

    public <T> @NonNull T registerWorkflows(
        @NonNull Class<T> interfaceClass, @NonNull T implementation) {
      return registerWorkflows(interfaceClass, implementation, "");
    }

    public <T> @NonNull T registerWorkflows(
        @NonNull Class<T> interfaceClass, @NonNull T implementation, @NonNull String instanceName) {
      registerClassWorkflows(interfaceClass, implementation, instanceName);

      return DBOSInvocationHandler.createProxy(
          interfaceClass, implementation, instanceName, () -> this.dbosExecutor.get());
    }

    private void registerInternals() {
      internalWorkflowsService =
          registerWorkflows(InternalWorkflowsService.class, new InternalWorkflowsServiceImpl());
      this.registerQueue(new Queue(Constants.DBOS_INTERNAL_QUEUE));
    }

    void clearRegistry() {
      workflowRegistry.clear();
      queueRegistry.clear();
      lifecycleRegistry.clear();

      registerInternals();
    }

    // package private methods for test purposes
    @Nullable DBOSExecutor getDbosExecutor() {
      return dbosExecutor.get();
    }

    public void setConfig(@NonNull DBOSConfig config) {
      if (this.config != null) {
        throw new IllegalStateException("DBOS has already been configured");
      }

      Objects.requireNonNull(config.appName(), "DBOSConfig.appName must not be null");
      if (config.dataSource() == null) {
        Objects.requireNonNull(config.databaseUrl(), "DBOSConfig.databaseUrl must not be null");
        Objects.requireNonNull(config.dbUser(), "DBOSConfig.dbUser must not be null");
        Objects.requireNonNull(config.dbPassword(), "DBOSConfig.dbPassword must not be null");
      }

      this.config = config;
    }

    public void launch() {
      if (this.config == null) {
        throw new IllegalStateException("DBOS must be configured before launch()");
      }
      var ver = DBOS.version();
      logger.info("Launching DBOS {}", ver == null ? "<unknown version>" : "v" + ver);

      if (dbosExecutor.get() == null) {
        var executor = new DBOSExecutor(config);

        if (dbosExecutor.compareAndSet(null, executor)) {
          executor.start(
              this,
              new HashSet<DBOSLifecycleListener>(this.lifecycleRegistry),
              workflowRegistry.getWorkflowSnapshot(),
              workflowRegistry.getInstanceSnapshot(),
              queueRegistry.getSnapshot());
        }
      }
    }

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
  }

  /**
   * Register all workflows and steps in the provided class instance
   *
   * @param <T> The interface type for the instance
   * @param interfaceClass The interface class for the workflows
   * @param implementation An implementation instance providing the workflow and step function code
   * @return A proxy, with interface {@literal <T>}, that provides durability for the workflow
   *     functions
   */
  public static <T> @NonNull T registerWorkflows(
      @NonNull Class<T> interfaceClass, @NonNull T implementation) {
    return ensureInstance().registerWorkflows(interfaceClass, implementation, "");
  }

  /**
   * Register all workflows and steps in the provided class instance
   *
   * @param <T> The interface type for the instance
   * @param interfaceClass The interface class for the workflows
   * @param implementation An implementation instance providing the workflow and step function code
   * @param instanceName Name of the instance, allowing multiple instances of the same class to be
   *     registered
   * @return A proxy, with interface {@literal <T>}, that provides durability for the workflow
   *     functions
   */
  public static <T> @NonNull T registerWorkflows(
      @NonNull Class<T> interfaceClass, @NonNull T implementation, @NonNull String instanceName) {
    return ensureInstance().registerWorkflows(interfaceClass, implementation, instanceName);
  }

  /**
   * Register a DBOS queue. This must be called on each queue prior to launch, so that recovery has
   * the queue options available.
   *
   * @param queue `Queue` to register
   * @return input queue
   */
  public static @NonNull Queue registerQueue(@NonNull Queue queue) {
    ensureInstance().registerQueue(queue);
    return queue;
  }

  /**
   * Register a set of DBOS queues. Each queue must be registered prior to launch, so that recovery
   * has the queue options available.
   *
   * @param queues collection of `Queue` instances to register
   */
  public static void registerQueues(@NonNull Queue... queues) {
    for (Queue queue : queues) {
      registerQueue(queue);
    }
  }

  /**
   * Register a lifecycle listener that receives callbacks when DBOS is launched or shut down
   *
   * @param listener
   */
  public static void registerLifecycleListener(@NonNull DBOSLifecycleListener listener) {
    ensureInstance().registerLifecycleListener(listener);
  }

  /**
   * Reinitializes the singleton instance of DBOS with config. For use in tests that reinitialize
   * DBOS @DBOSConfig config dbos configuration
   */
  public static synchronized Instance reinitialize(DBOSConfig config) {
    if (config.migrate()) {
      MigrationManager.runMigrations(config);
    }
    var instance = new Instance();
    instance.setConfig(config);
    instance.registerInternals();
    globalInstance = instance;
    return instance;
  }

  /**
   * Initializes the singleton instance of DBOS with config. Should be called once during app
   * startup, before launch. @DBOSConfig config dbos configuration
   */
  public static synchronized Instance configure(DBOSConfig config) {
    var instance = ensureInstance();
    instance.setConfig(config);
    instance.registerInternals();
    if (config.migrate()) {
      MigrationManager.runMigrations(config);
    }
    return instance;
  }

  /**
   * Launch DBOS, and start recovery. All workflows, queues, and other objects should be registered
   * before launch
   */
  public static void launch() {
    ensureInstance().launch();
  }

  /**
   * Shut down DBOS. This method should only be used in test environments, where DBOS is used
   * multiple times in the same JVM.
   */
  public static void shutdown() {
    if (globalInstance != null) globalInstance.shutdown();
  }

  private static @Nullable Instance globalInstance = null;

  public static @Nullable Instance instance() {
    return globalInstance;
  }

  private static synchronized Instance ensureInstance() {
    if (globalInstance == null) {
      globalInstance = new Instance();
    }
    return globalInstance;
  }

  static DBOSExecutor executor(String caller) {
    var inst = instance();
    if (inst == null)
      throw new IllegalStateException(
          String.format("Cannot call %s before DBOS is created", caller));
    var executor = inst.getDbosExecutor();
    if (executor == null)
      throw new IllegalStateException(
          String.format("Cannot call %s before DBOS is launched", caller));
    return executor;
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
   * Retrieve a queue definition
   *
   * @param queueName Name of the queue
   * @return Queue definition for given `queueName`
   */
  public static @NonNull Optional<Queue> getQueue(@NonNull String queueName) {
    return executor("getQueue").getQueue(queueName);
  }

  /**
   * Durable sleep. Use this instead of Thread.sleep, especially in workflows. On restart or during
   * recovery the original expected wakeup time is honoured as opposed to sleeping all over again.
   *
   * @param duration amount of time to sleep
   */
  public static void sleep(@NonNull Duration duration) {
    if (!inWorkflow()) {
      try {
        Thread.sleep(duration.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else if (inStep()) {
      try {
        Thread.sleep(duration.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      executor("sleep").sleep(duration);
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
  public static <T, E extends Exception> @NonNull WorkflowHandle<T, E> startWorkflow(
      @NonNull ThrowingSupplier<T, E> supplier, @NonNull StartWorkflowOptions options) {
    return executor("startWorkflow").startWorkflow(supplier, options);
  }

  /**
   * Start or enqueue a workflow with default options
   *
   * @param <T> Return type of the workflow
   * @param <E> Type of checked exception thrown by the workflow, if any
   * @param supplier A lambda that calls exactly one workflow function
   * @return A handle to the enqueued or running workflow
   */
  public static <T, E extends Exception> @NonNull WorkflowHandle<T, E> startWorkflow(
      @NonNull ThrowingSupplier<T, E> supplier) {
    return startWorkflow(supplier, new StartWorkflowOptions());
  }

  /**
   * Start or enqueue a workflow with no return value
   *
   * @param <E> Type of checked exception thrown by the workflow, if any
   * @param runnable A lambda that calls exactly one workflow function
   * @param options Start workflow options
   * @return A handle to the enqueued or running workflow
   */
  public static <E extends Exception> @NonNull WorkflowHandle<Void, E> startWorkflow(
      @NonNull ThrowingRunnable<E> runnable, @NonNull StartWorkflowOptions options) {
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
  public static <E extends Exception> @NonNull WorkflowHandle<Void, E> startWorkflow(
      @NonNull ThrowingRunnable<E> runnable) {
    return startWorkflow(runnable, new StartWorkflowOptions());
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
  public static <T, E extends Exception> T getResult(@NonNull String workflowId) throws E {
    return executor("getResult").<T, E>getResult(workflowId);
  }

  /**
   * Get the status of a workflow
   *
   * @param workflowId ID of the workflow to query
   * @return Current workflow status for the provided workflowId, or null.
   */
  public static @Nullable WorkflowStatus getWorkflowStatus(@NonNull String workflowId) {
    return executor("getWorkflowStatus").getWorkflowStatus(workflowId);
  }

  /**
   * Get the serialization format of the current workflow context.
   *
   * @return the serialization format name (e.g., "portable_json", "java_jackson"), or null if not
   *     in a workflow context or using default serialization
   */
  public static @Nullable String getSerialization() {
    var ctx = DBOSContextHolder.get();
    return ctx != null ? ctx.getSerialization() : null;
  }

  /**
   * Send a message to a workflow
   *
   * @param destinationId recipient of the message
   * @param message message to be sent
   * @param topic topic to which the message is send
   * @param idempotencyKey optional idempotency key for exactly-once send
   */
  public static void send(
      @NonNull String destinationId,
      @NonNull Object message,
      @NonNull String topic,
      @Nullable String idempotencyKey) {
    send(destinationId, message, topic, idempotencyKey, null);
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
  public static void send(
      @NonNull String destinationId,
      @NonNull Object message,
      @NonNull String topic,
      @Nullable String idempotencyKey,
      @Nullable SerializationStrategy serialization) {
    String serializationFormat = serialization != null ? serialization.formatName() : null;
    executor("send")
        .send(
            destinationId,
            message,
            topic,
            instance().internalWorkflowsService,
            idempotencyKey,
            serializationFormat);
  }

  /**
   * Send a message to a workflow
   *
   * @param destinationId recipient of the message
   * @param message message to be sent
   * @param topic topic to which the message is send
   */
  public static void send(
      @NonNull String destinationId, @NonNull Object message, @NonNull String topic) {
    DBOS.send(destinationId, message, topic, null, null);
  }

  /**
   * Get a message sent to a particular topic
   *
   * @param topic the topic whose message to get
   * @param timeout duration after which the call times out
   * @return the message if there is one or else null
   */
  public static @Nullable Object recv(@NonNull String topic, @NonNull Duration timeout) {
    return executor("recv").recv(topic, timeout);
  }

  /**
   * Call within a workflow to publish a key value pair. Uses the workflow's serialization format.
   *
   * @param key identifier for published data
   * @param value data that is published
   */
  public static void setEvent(@NonNull String key, @NonNull Object value) {
    setEvent(key, value, null);
  }

  /**
   * Call within a workflow to publish a key value pair with a specific serialization strategy.
   *
   * @param key identifier for published data
   * @param value data that is published
   * @param serialization serialization strategy to use (null to use workflow's default)
   */
  public static void setEvent(
      @NonNull String key, @NonNull Object value, @Nullable SerializationStrategy serialization) {
    // If no explicit serialization specified, use the workflow context's serialization
    String serializationFormat;
    if (serialization != null) {
      serializationFormat = serialization.formatName();
    } else {
      serializationFormat = getSerialization();
    }
    executor("setEvent").setEvent(key, value, serializationFormat);
  }

  /**
   * Get the data published by a workflow
   *
   * @param workflowId id of the workflow who data is to be retrieved
   * @param key identifies the data
   * @param timeout time to wait for data before timing out
   * @return the published value or null
   */
  public static @Nullable Object getEvent(
      @NonNull String workflowId, @NonNull String key, @NonNull Duration timeout) {
    logger.debug("Received getEvent for {} {}", workflowId, key);

    return executor("getEvent").getEvent(workflowId, key, timeout);
  }

  /**
   * Run the provided function as a step; this variant is for functions with a return value
   *
   * @param <E> Checked exception thrown by the step, if any
   * @param stepfunc function or lambda to run
   * @param opts step name, and retry options for running the step
   * @throws E
   */
  public static <T, E extends Exception> T runStep(
      @NonNull ThrowingSupplier<T, E> stepfunc, @NonNull StepOptions opts) throws E {

    return executor("runStep").runStepInternal(stepfunc, opts, null);
  }

  /**
   * Run the provided function as a step; this variant is for functions with a return value
   *
   * @param <E> Checked exception thrown by the step, if any
   * @param stepfunc function or lambda to run
   * @param name name of the step, for tracing and to record in the system database
   * @throws E
   */
  public static <T, E extends Exception> T runStep(
      @NonNull ThrowingSupplier<T, E> stepfunc, @NonNull String name) throws E {

    return executor("runStep").runStepInternal(stepfunc, new StepOptions(name), null);
  }

  /**
   * Run the provided function as a step; this variant is for functions with no return value
   *
   * @param <E> Checked exception thrown by the step, if any
   * @param stepfunc function or lambda to run
   * @param opts step name, and retry options for running the step
   * @throws E
   */
  public static <E extends Exception> void runStep(
      @NonNull ThrowingRunnable<E> stepfunc, @NonNull StepOptions opts) throws E {
    executor("runStep")
        .runStepInternal(
            () -> {
              stepfunc.execute();
              return null;
            },
            opts,
            null);
  }

  /**
   * Run the provided function as a step; this variant is for functions with no return value
   *
   * @param <E> Checked exception thrown by the step, if any
   * @param stepfunc function or lambda to run
   * @param name Name of the step, for tracing and recording in the system database
   * @throws E
   */
  public static <E extends Exception> void runStep(
      @NonNull ThrowingRunnable<E> stepfunc, @NonNull String name) throws E {
    runStep(stepfunc, new StepOptions(name));
  }

  /**
   * Resume a workflow starting from the step after the last complete step
   *
   * @param <T> Return type of the workflow function
   * @param <E> Checked exception thrown by the workflow function, if any
   * @param workflowId id of the workflow
   * @return A handle to the workflow
   */
  public static <T, E extends Exception> @NonNull WorkflowHandle<T, E> resumeWorkflow(
      @NonNull String workflowId) {
    return executor("resumeWorkflow").resumeWorkflow(workflowId);
  }

  /***
   *
   * Cancel the workflow. After this function is called, the next step (not the
   * current one) will not execute
   *
   * @param workflowId ID of the workflow to cancel
   */
  public static void cancelWorkflow(@NonNull String workflowId) {
    executor("cancelWorkflow").cancelWorkflow(workflowId);
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
  public static <T, E extends Exception> @NonNull WorkflowHandle<T, E> forkWorkflow(
      @NonNull String workflowId, int startStep, @NonNull ForkOptions options) {
    return executor("forkWorkflow").forkWorkflow(workflowId, startStep, options);
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
  public static <T, E extends Exception> @NonNull WorkflowHandle<T, E> forkWorkflow(
      @NonNull String workflowId, int startStep) {
    return forkWorkflow(workflowId, startStep, new ForkOptions());
  }

  /**
   * Deletes a workflow from the system. Does not delete child workflows.
   *
   * @param workflowId the unique identifier of the workflow to delete. Must not be null.
   * @throws IllegalArgumentException if workflowId is null
   */
  public static void deleteWorkflow(@NonNull String workflowId) {
    deleteWorkflow(workflowId, false);
  }

  /**
   * Deletes a workflow and optionally its child workflows from the system.
   *
   * @param workflowId the unique identifier of the workflow to delete. Must not be null.
   * @param deleteChildren if true, also deletes all child workflows associated with the specified
   *     workflow; if false, only deletes the specified workflow
   * @throws IllegalArgumentException if workflowId is null
   */
  public static void deleteWorkflow(@NonNull String workflowId, boolean deleteChildren) {
    executor("deleteWorkflow").deleteWorkflow(workflowId, deleteChildren);
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
  public static <T, E extends Exception> @NonNull WorkflowHandle<T, E> retrieveWorkflow(
      @NonNull String workflowId) {
    return executor("retrieveWorkflow").retrieveWorkflow(workflowId);
  }

  /**
   * List all workflows
   *
   * @param input {@link ListWorkflowsInput} parameters to query workflows
   * @return a list of workflow status {@link WorkflowStatus}
   */
  public static @NonNull List<WorkflowStatus> listWorkflows(@NonNull ListWorkflowsInput input) {
    return executor("listWorkflows").listWorkflows(input);
  }

  /**
   * List the steps in the workflow
   *
   * @param workflowId Id of the workflow whose steps to return
   * @return list of step information {@link StepInfo}
   */
  public static @NonNull List<StepInfo> listWorkflowSteps(@NonNull String workflowId) {
    return executor("listWorkflowSteps").listWorkflowSteps(workflowId);
  }

  /**
   * Get all workflows registered with DBOS.
   *
   * @return list of all registered workflow methods
   */
  public static @NonNull Collection<RegisteredWorkflow> getRegisteredWorkflows() {
    return executor("getRegisteredWorkflows").getWorkflows();
  }

  /**
   * Get all workflow classes registered with DBOS.
   *
   * @return list of all class instances containing registered workflow methods
   */
  public static @NonNull Collection<RegisteredWorkflowInstance> getRegisteredWorkflowInstances() {
    return executor("getRegisteredWorkflowInstances").getInstances();
  }

  /**
   * Execute a workflow based on registration and arguments. This is expected to be used by generic
   * callers, not app code.
   *
   * @param regWorkflow Registration of the workflow. @see getRegisteredWorkflows
   * @param args Workflow function arguments
   * @param options Execution options, such as ID, queue, and timeout/deadline
   * @return WorkflowHandle to the executed workflow
   */
  public static WorkflowHandle<?, ?> startWorkflow(
      RegisteredWorkflow regWorkflow, Object[] args, StartWorkflowOptions options) {
    return executor("startWorkflow").startWorkflow(regWorkflow, args, options);
  }

  /**
   * Get a system database record stored by an external service A unique value is stored per
   * combination of service, workflowName, and key
   *
   * @param service Identity of the service maintaining the record
   * @param workflowName Fully qualified name of the workflow
   * @param key Key assigned within the service+workflow
   * @return Value associated with the service+workflow+key combination
   */
  public static Optional<ExternalState> getExternalState(
      String service, String workflowName, String key) {
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
  public static ExternalState upsertExternalState(ExternalState state) {
    return executor("upsertExternalState").upsertExternalState(state);
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
  public static boolean patch(@NonNull String patchName) {
    return executor("patch").patch(patchName);
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
  public static boolean deprecatePatch(@NonNull String patchName) {
    return executor("deprecatePatch").deprecatePatch(patchName);
  }
}
