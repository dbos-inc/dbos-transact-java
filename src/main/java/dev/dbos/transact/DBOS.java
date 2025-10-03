package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.ThrowingRunnable;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.internal.DBOSInvocationHandler;
import dev.dbos.transact.internal.QueueRegistry;
import dev.dbos.transact.internal.WorkflowRegistry;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.queue.ListQueuedWorkflowsInput;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.queue.RateLimit;
import dev.dbos.transact.scheduled.SchedulerService;
import dev.dbos.transact.scheduled.SchedulerService.ScheduledInstance;
import dev.dbos.transact.tempworkflows.InternalWorkflowsService;
import dev.dbos.transact.tempworkflows.InternalWorkflowsServiceImpl;
import dev.dbos.transact.workflow.*;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOS {

  private static final Logger logger = LoggerFactory.getLogger(DBOS.class);

  private final WorkflowRegistry workflowRegistry = new WorkflowRegistry();
  private final QueueRegistry queueRegistry = new QueueRegistry();
  private final List<ScheduledInstance> scheduledWorkflows = new ArrayList<>();

  private final DBOSConfig config;

  private InternalWorkflowsService internalWorkflowsService;

  private final AtomicReference<DBOSExecutor> dbosExecutor = new AtomicReference<>();

  private DBOS(DBOSConfig config) {
    this.config = config;

    DBOSContextHolder.clear();
  }

  /**
   * Initializes the singleton instance of DBOS with config. Should be called once during app
   * startup. @DBOSConfig config dbos configuration
   */
  public static synchronized DBOS initialize(DBOSConfig config) {
    if (config.migration()) {
      MigrationManager.runMigrations(config);
    }
    var instance = new DBOS(config);
    instance.registerInternals();
    return instance;
  }

  // package private methods for test purposes
  DBOSExecutor getDbosExecutor() {
    return dbosExecutor.get();
  }

  void clearRegistry() {
    workflowRegistry.clear();
    queueRegistry.clear();
    scheduledWorkflows.clear();

    registerInternals();
  }

  private void registerClassWorkflows(
      Class<?> interfaceClass, Object implementation, String instanceName) {
    Objects.requireNonNull(interfaceClass);
    Objects.requireNonNull(implementation);
    Objects.requireNonNull(instanceName);
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

  public RegisteredWorkflow getWorkflow(
      String className, String instanceName, String workflowName) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot retrieve workflow before launch");
    }

    return executor.getWorkflow(className, instanceName, workflowName);
  }

  public Optional<Queue> getQueue(String queueName) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot retrieve queue before launch");
    }

    return executor.getQueue(queueName);
  }

  void registerQueue(Queue queue) {
    if (dbosExecutor.get() != null) {
      throw new IllegalStateException("Cannot build a queue after DBOS is launched");
    }

    queueRegistry.register(queue);
  }

  public <T> WorkflowBuilder<T> Workflow() {
    return new WorkflowBuilder<>(this);
  }

  public QueueBuilder Queue(String name) {
    return new QueueBuilder(this, name);
  }

  private void registerInternals() {
    internalWorkflowsService =
        this.<InternalWorkflowsService>Workflow()
            .interfaceClass(InternalWorkflowsService.class)
            .implementation(new InternalWorkflowsServiceImpl(this))
            .build();

    this.Queue(Constants.DBOS_INTERNAL_QUEUE).build();
    this.Queue(Constants.DBOS_SCHEDULER_QUEUE).build();
  }

  // inner builder class for workflows
  public static class WorkflowBuilder<T> {
    private final DBOS dbos;
    private Class<T> interfaceClass;
    private Object implementation;
    private String instanceName = "";

    WorkflowBuilder(DBOS dbos) {
      this.dbos = dbos;
    }

    public WorkflowBuilder<T> interfaceClass(Class<T> iface) {
      this.interfaceClass = iface;
      return this;
    }

    public WorkflowBuilder<T> implementation(Object impl) {
      this.implementation = impl;
      return this;
    }

    public WorkflowBuilder<T> instanceName(String name) {
      this.instanceName = name;
      return this;
    }

    public T build() {
      dbos.registerClassWorkflows(interfaceClass, implementation, instanceName);

      return DBOSInvocationHandler.createProxy(
          interfaceClass, implementation, instanceName, () -> dbos.dbosExecutor.get());
    }
  }

  public static class QueueBuilder {

    private final DBOS dbos;
    private final String name;

    private int concurrency;
    private int workerConcurrency;
    private RateLimit limit;
    private boolean priorityEnabled = false;

    /**
     * Constructor for the Builder, taking the required 'name' field.
     *
     * @param name The name of the queue.
     */
    QueueBuilder(DBOS dbos, String name) {
      this.dbos = dbos;
      this.name = name;
    }

    public QueueBuilder concurrency(Integer concurrency) {
      this.concurrency = concurrency;
      return this;
    }

    public QueueBuilder workerConcurrency(Integer workerConcurrency) {
      this.workerConcurrency = workerConcurrency;
      return this;
    }

    public QueueBuilder limit(int limit, double period) {
      this.limit = new RateLimit(limit, period);
      return this;
    }

    public QueueBuilder priorityEnabled(boolean priorityEnabled) {
      this.priorityEnabled = priorityEnabled;
      return this;
    }

    public Queue build() {
      Queue queue = Queue.createQueue(name, concurrency, workerConcurrency, limit, priorityEnabled);
      dbos.registerQueue(queue);
      return queue;
    }
  }

  public void launch() {
    logger.debug("launch()");

    if (dbosExecutor.get() == null) {
      var executor = new DBOSExecutor(config);

      if (dbosExecutor.compareAndSet(null, executor)) {
        executor.start(
            this,
            workflowRegistry.getSnapshot(),
            queueRegistry.getSnapshot(),
            List.copyOf(scheduledWorkflows));
      }
    }
  }

  public void shutdown() throws Exception {
    logger.debug("shutdown()");

    var current = dbosExecutor.getAndSet(null);
    if (current != null) {
      current.close();
    }
  }

  /**
   * Scans the class for all methods that have Workflow and Scheduled annotations and schedules them
   * for execution
   *
   * @param implementation instance of a class
   */
  public void scheduleWorkflow(Object implementation) {
    var expectedParams = new Class<?>[] {Instant.class, Instant.class};

    for (Method method : implementation.getClass().getDeclaredMethods()) {
      Workflow wfAnnotation = method.getAnnotation(Workflow.class);
      if (wfAnnotation == null) {
        continue;
      }

      Scheduled scheduled = method.getAnnotation(Scheduled.class);
      if (scheduled == null) {
        continue;
      }

      var paramTypes = method.getParameterTypes();
      if (!Arrays.equals(paramTypes, expectedParams)) {
        throw new IllegalArgumentException(
            "Scheduled workflow must have parameters (Instant scheduledTime, Instant actualTime)");
      }

      String wfName = registerWorkflowMethod(wfAnnotation, implementation, "", method);
      var scheduledWF =
          SchedulerService.makeScheduledInstance(
              implementation.getClass().getName(), wfName, implementation, scheduled.cron());
      this.scheduledWorkflows.add(scheduledWF);
    }
  }

  /**
   * Send a message to a workflow
   *
   * @param destinationId recipient of the message
   * @param message message to be sent
   * @param topic topic to which the message is send
   */
  public void send(String destinationId, Object message, String topic) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot send before launch");
    }
    executor.send(destinationId, message, topic, internalWorkflowsService);
  }

  /**
   * Get a message sent to a particular topic
   *
   * @param topic the topic whose message to get
   * @param timeoutSeconds time in seconds after which the call times out
   * @return the message if there is one or else null
   */
  public Object recv(String topic, float timeoutSeconds) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot recv before launch");
    }
    return executor.recv(topic, timeoutSeconds);
  }

  /**
   * Call within a workflow to publish a key value pair
   *
   * @param key identifier for published data
   * @param value data that is published
   */
  public void setEvent(String key, Object value) {
    logger.info("Received setEvent for key {}", key);

    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot setEvent before launch");
    }

    executor.setEvent(key, value);
  }

  /**
   * Get the data published by a workflow
   *
   * @param workflowId id of the workflow who data is to be retrieved
   * @param key identifies the data
   * @param timeOut time in seconds to wait for data
   * @return the published value or null
   */
  public Object getEvent(String workflowId, String key, float timeOut) {
    logger.info("Received getEvent for {} {}", workflowId, key);

    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot getEvent before launch");
    }

    return executor.getEvent(workflowId, key, timeOut);
  }

  /**
   * Durable sleep. When you are in a workflow, use this instead of Thread.sleep. On restart or
   * during recovery the original expected wakeup time is honoured as opposed to sleeping all over
   * again.
   *
   * @param seconds in seconds
   */
  public void sleep(float seconds) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot sleep before launch");
    }

    executor.sleep(seconds);
  }

  public <T, E extends Exception> T runStep(ThrowingSupplier<T, E> stepfunc, StepOptions opts)
      throws E {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot runStep before launch");
    }

    return executor.runStepI(stepfunc, opts);
  }

  public <E extends Exception> void runStep(ThrowingRunnable<E> stepfunc, StepOptions opts)
      throws E {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot runStep before launch");
    }
    executor.runStepI(
        () -> {
          stepfunc.execute();
          return null;
        },
        opts);
  }

  /**
   * Resume a workflow starting from the step after the last complete step
   *
   * @param workflowId id of the workflow
   * @return A handle to the workflow
   */
  public <T, E extends Exception> WorkflowHandle<T, E> resumeWorkflow(String workflowId) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot resumeWorkflow before launch");
    }

    return executor.resumeWorkflow(workflowId);
  }

  /***
   *
   * Cancel the workflow. After this function is called, the next step (not the
   * current one) will not execute
   *
   * @param workflowId
   */

  public void cancelWorkflow(String workflowId) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot cancelWorkflow before launch");
    }

    executor.cancelWorkflow(workflowId);
  }

  /**
   * Fork the workflow. Re-execute with another Id from the step provided. Steps prior to the
   * provided step are copied over
   *
   * @param workflowId Original workflow Id
   * @param startStep Start execution from this step. Prior steps copied over
   * @param options {@link ForkOptions} containing forkedWorkflowId, applicationVersion, timeout
   * @return handle to the workflow
   */
  public <T, E extends Exception> WorkflowHandle<T, E> forkWorkflow(
      String workflowId, int startStep, ForkOptions options) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot forkWorkflow before launch");
    }

    return executor.forkWorkflow(workflowId, startStep, options);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> startWorkflow(
      ThrowingSupplier<T, E> supplier, StartWorkflowOptions options) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot startWorkflow before launch");
    }

    return executor.startWorkflow(supplier, options);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> startWorkflow(
      ThrowingSupplier<T, E> supplier) {
    return startWorkflow(supplier, new StartWorkflowOptions());
  }

  public <E extends Exception> WorkflowHandle<Void, E> startWorkflow(
      ThrowingRunnable<E> runnable, StartWorkflowOptions options) {
    return startWorkflow(
        () -> {
          runnable.execute();
          return null;
        },
        options);
  }

  public <E extends Exception> WorkflowHandle<Void, E> startWorkflow(ThrowingRunnable<E> runnable) {
    return startWorkflow(runnable, new StartWorkflowOptions());
  }

  public <T, E extends Exception> WorkflowHandle<T, E> retrieveWorkflow(String workflowId) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot startWorkflow before launch");
    }

    return executor.retrieveWorkflow(workflowId);
  }

  /**
   * List all workflows
   *
   * @param input {@link ListWorkflowsInput} parameters to query workflows
   * @return a list of workflow status {@link WorkflowStatus}
   */
  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot listWorkflows before launch");
    }
    return executor.listWorkflows(input);
  }

  /**
   * List the steps in the workflow
   *
   * @param workflowId Id of the workflow whose steps to return
   * @return list of step information {@link StepInfo}
   */
  public List<StepInfo> listWorkflowSteps(String workflowId) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot listWorkflowSteps before launch");
    }
    return executor.listWorkflowSteps(workflowId);
  }

  /**
   * List workflows queued
   *
   * @param query parameters to query by
   * @param loadInput Whether to load input or not
   * @return list of workflow statuses {@link WorkflowStatus}
   */
  public List<WorkflowStatus> listQueuedWorkflows(
      ListQueuedWorkflowsInput query, boolean loadInput) {
    var executor = dbosExecutor.get();
    if (executor == null) {
      throw new IllegalStateException("cannot listQueuedWorkflows before launch");
    }
    return executor.listQueuedWorkflows(query, loadInput);
  }
}
