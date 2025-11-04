package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.ThrowingRunnable;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.internal.DBOSInvocationHandler;
import dev.dbos.transact.internal.QueueRegistry;
import dev.dbos.transact.internal.WorkflowRegistry;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.tempworkflows.InternalWorkflowsService;
import dev.dbos.transact.tempworkflows.InternalWorkflowsServiceImpl;
import dev.dbos.transact.workflow.*;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

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

  public static class Instance {
    private final WorkflowRegistry workflowRegistry = new WorkflowRegistry();
    private final QueueRegistry queueRegistry = new QueueRegistry();

    private DBOSConfig config;

    private InternalWorkflowsService internalWorkflowsService;

    private final AtomicReference<DBOSExecutor> dbosExecutor = new AtomicReference<>();

    private Instance() {
      DBOSContextHolder.clear(); // CB: Why
    }

    private void registerClassWorkflows(
        Class<?> interfaceClass, Object implementation, String instanceName) {
      Objects.requireNonNull(interfaceClass, "interfaceClass must not be null");
      Objects.requireNonNull(implementation, "implementation must not be null");
      instanceName = Objects.requireNonNullElse(instanceName, "");
      if (!interfaceClass.isInterface()) {
        throw new IllegalArgumentException("interfaceClass must be an interface");
      }
      if (dbosExecutor.get() != null) {
        throw new IllegalStateException("Cannot register workflow after DBOS is launched");
      }

      Method[] methods = implementation.getClass().getDeclaredMethods();
      for (Method method : methods) {
        Workflow wfAnnotation = method.getAnnotation(Workflow.class);
        if (wfAnnotation != null) {
          method.setAccessible(true); // In case it's not public
          registerWorkflowMethod(wfAnnotation, implementation, instanceName, method);
        }
      }
    }

    private String registerWorkflowMethod(
        Workflow wfTag, Object target, String instanceName, Method method) {
      if (dbosExecutor.get() != null) {
        throw new IllegalStateException("Cannot register workflow after DBOS is launched");
      }

      String name = wfTag.name().isEmpty() ? method.getName() : wfTag.name();
      workflowRegistry.register(
          target.getClass().getName(),
          name,
          target,
          instanceName,
          method,
          wfTag.maxRecoveryAttempts());
      return name;
    }

    void registerQueue(Queue queue) {
      if (dbosExecutor.get() != null) {
        throw new IllegalStateException("Cannot build a queue after DBOS is launched");
      }

      queueRegistry.register(queue);
    }

    public <T> T registerWorkflows(Class<T> interfaceClass, T implementation) {
      return registerWorkflows(interfaceClass, implementation, "");
    }

    public <T> T registerWorkflows(Class<T> interfaceClass, T implementation, String instanceName) {
      registerClassWorkflows(interfaceClass, implementation, instanceName);

      return DBOSInvocationHandler.createProxy(
          interfaceClass, implementation, instanceName, () -> this.dbosExecutor.get());
    }

    private void registerInternals() {
      internalWorkflowsService =
          registerWorkflows(InternalWorkflowsService.class, new InternalWorkflowsServiceImpl());
      this.registerQueue(new Queue(Constants.DBOS_INTERNAL_QUEUE));
      this.registerQueue(new Queue(Constants.DBOS_SCHEDULER_QUEUE));
    }

    void clearRegistry() {
      workflowRegistry.clear();
      queueRegistry.clear();

      registerInternals();
    }

    // package private methods for test purposes
    DBOSExecutor getDbosExecutor() {
      return dbosExecutor.get();
    }

    public void setConfig(DBOSConfig config) {
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
      var pkg = DBOS.class.getPackage();
      var ver = pkg == null ? null : pkg.getImplementationVersion();
      logger.info("Launching DBOS {}", ver == null ? "<unknown version>" : "v" + ver);

      if (dbosExecutor.get() == null) {
        var executor = new DBOSExecutor(config);

        if (dbosExecutor.compareAndSet(null, executor)) {
          executor.start(this, workflowRegistry.getSnapshot(), queueRegistry.getSnapshot());
        }
      }
    }

    public void shutdown() {
      var current = dbosExecutor.get();
      if (current != null) {
        current.close();
      }
      dbosExecutor.compareAndSet(current, null);
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
  public static <T> T registerWorkflows(Class<T> interfaceClass, T implementation) {
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
  public static <T> T registerWorkflows(
      Class<T> interfaceClass, T implementation, String instanceName) {
    return ensureInstance().registerWorkflows(interfaceClass, implementation, instanceName);
  }

  /**
   * Register a DBOS queue. This must be called on each queue prior to launch, so that recovery has
   * the queue options available.
   *
   * @param queue `Queue` to register
   * @return input queue
   */
  public static Queue registerQueue(Queue queue) {
    ensureInstance().registerQueue(queue);
    return queue;
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

  private static Instance globalInstance = null;

  public static Instance instance() {
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
  public static String workflowId() {
    return DBOSContext.workflowId();
  }

  /**
   * Get the ID of the current running step, or `null` if a workflow step is not in progress
   *
   * @return Current step ID number
   */
  public static Integer stepId() {
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
  public static Optional<Queue> getQueue(String queueName) {
    return executor("getQueue").getQueue(queueName);
  }

  /**
   * Durable sleep. Use this instead of Thread.sleep, especially in workflows. On restart or during
   * recovery the original expected wakeup time is honoured as opposed to sleeping all over again.
   *
   * @param duration amount of time to sleep
   */
  public static void sleep(Duration duration) {
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
  public static <T, E extends Exception> WorkflowHandle<T, E> startWorkflow(
      ThrowingSupplier<T, E> supplier, StartWorkflowOptions options) {
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
  public static <T, E extends Exception> WorkflowHandle<T, E> startWorkflow(
      ThrowingSupplier<T, E> supplier) {
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
  public static <E extends Exception> WorkflowHandle<Void, E> startWorkflow(
      ThrowingRunnable<E> runnable, StartWorkflowOptions options) {
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
  public static <E extends Exception> WorkflowHandle<Void, E> startWorkflow(
      ThrowingRunnable<E> runnable) {
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
  public static <T, E extends Exception> T getResult(String workflowId) throws E {
    return executor("getResult").<T, E>getResult(workflowId);
  }

  /**
   * Get the status of a workflow
   *
   * @param workflowId ID of the workflow to query
   * @return Current workflow status for the provided workflowId, or null.
   */
  public static WorkflowStatus getWorkflowStatus(String workflowId) {
    return executor("getWorkflowStatus").getWorkflowStatus(workflowId);
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
      String destinationId, Object message, String topic, String idempotencyKey) {
    executor("send")
        .send(destinationId, message, topic, instance().internalWorkflowsService, idempotencyKey);
  }

  /**
   * Send a message to a workflow
   *
   * @param destinationId recipient of the message
   * @param message message to be sent
   * @param topic topic to which the message is send
   */
  public static void send(String destinationId, Object message, String topic) {
    DBOS.send(destinationId, message, topic, null);
  }

  /**
   * Get a message sent to a particular topic
   *
   * @param topic the topic whose message to get
   * @param timeout duration after which the call times out
   * @return the message if there is one or else null
   */
  public static Object recv(String topic, Duration timeout) {
    return executor("recv").recv(topic, timeout);
  }

  /**
   * Call within a workflow to publish a key value pair
   *
   * @param key identifier for published data
   * @param value data that is published
   */
  public static void setEvent(String key, Object value) {
    executor("setEvent").setEvent(key, value);
  }

  /**
   * Get the data published by a workflow
   *
   * @param workflowId id of the workflow who data is to be retrieved
   * @param key identifies the data
   * @param timeout time to wait for data before timing out
   * @return the published value or null
   */
  public static Object getEvent(String workflowId, String key, Duration timeout) {
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
      ThrowingSupplier<T, E> stepfunc, StepOptions opts) throws E {

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
  public static <T, E extends Exception> T runStep(ThrowingSupplier<T, E> stepfunc, String name)
      throws E {

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
  public static <E extends Exception> void runStep(ThrowingRunnable<E> stepfunc, StepOptions opts)
      throws E {
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
  public static <E extends Exception> void runStep(ThrowingRunnable<E> stepfunc, String name)
      throws E {
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
  public static <T, E extends Exception> WorkflowHandle<T, E> resumeWorkflow(String workflowId) {
    return executor("resumeWorkflow").resumeWorkflow(workflowId);
  }

  /***
   *
   * Cancel the workflow. After this function is called, the next step (not the
   * current one) will not execute
   *
   * @param workflowId ID of the workflow to cancel
   */
  public static void cancelWorkflow(String workflowId) {
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
  public static <T, E extends Exception> WorkflowHandle<T, E> forkWorkflow(
      String workflowId, int startStep, ForkOptions options) {
    return executor("forkWorkflow").forkWorkflow(workflowId, startStep, options);
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
  public static <T, E extends Exception> WorkflowHandle<T, E> retrieveWorkflow(String workflowId) {
    return executor("retrieveWorkflow").retrieveWorkflow(workflowId);
  }

  /**
   * List all workflows
   *
   * @param input {@link ListWorkflowsInput} parameters to query workflows
   * @return a list of workflow status {@link WorkflowStatus}
   */
  public static List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return executor("listWorkflows").listWorkflows(input);
  }

  /**
   * List the steps in the workflow
   *
   * @param workflowId Id of the workflow whose steps to return
   * @return list of step information {@link StepInfo}
   */
  public static List<StepInfo> listWorkflowSteps(String workflowId) {
    return executor("listWorkflowSteps").listWorkflowSteps(workflowId);
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
}
