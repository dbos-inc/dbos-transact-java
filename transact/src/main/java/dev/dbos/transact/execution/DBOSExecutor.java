package dev.dbos.transact.execution;

import dev.dbos.transact.AlertHandler;
import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.admin.AdminServer;
import dev.dbos.transact.conductor.Conductor;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.context.WorkflowInfo;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.database.GetWorkflowEventContext;
import dev.dbos.transact.database.Result;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.database.WorkflowInitResult;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.exceptions.DBOSWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException;
import dev.dbos.transact.internal.AppVersionComputer;
import dev.dbos.transact.internal.DBOSInvocationHandler;
import dev.dbos.transact.internal.Invocation;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.tempworkflows.InternalWorkflowsService;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.StepOptions;
import dev.dbos.transact.workflow.Timeout;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;
import dev.dbos.transact.workflow.internal.WorkflowHandleFuture;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSExecutor implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(DBOSExecutor.class);

  private final DBOSConfig config;
  private final DBOSSerializer serializer;

  private boolean dbosCloud;
  private String appVersion;
  private String executorId;
  private String appId;

  private Set<DBOSLifecycleListener> listeners;
  private Map<String, RegisteredWorkflow> workflowMap;
  private Map<String, RegisteredWorkflowInstance> instanceMap;
  private List<Queue> queues;
  private ConcurrentHashMap<String, Boolean> workflowsInProgress = new ConcurrentHashMap<>();

  private SystemDatabase systemDatabase;
  private QueueService queueService;
  private SchedulerService schedulerService;
  private AdminServer adminServer;
  private Conductor conductor;
  private ExecutorService executorService;
  private ScheduledExecutorService timeoutScheduler;
  private AlertHandler alertHandler;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  public DBOSExecutor(DBOSConfig config) {
    this.config = config;
    this.serializer = config.serializer();

    if (config.conductorKey() != null && config.executorId() != null) {
      throw new IllegalArgumentException(
          "DBOSConfig.executorId cannot be specified when using Conductor");
    }

    appVersion = Objects.requireNonNullElse(System.getenv("DBOS__APPVERSION"), "");
    appId = Objects.requireNonNullElse(System.getenv("DBOS__APPID"), "");
    executorId = Objects.requireNonNullElse(System.getenv("DBOS__VMID"), "local");
    dbosCloud = Objects.requireNonNullElse(System.getenv("DBOS__CLOUD"), "").equals("true");

    if (!dbosCloud) {
      if (config.enablePatching()) {
        appVersion = "PATCHING_ENABLED";
      }

      if (config.appVersion() != null) {
        appVersion = config.appVersion();
      }
      if (config.executorId() != null) {
        executorId = config.executorId();
      }
    }
  }

  public void start(
      DBOS.Instance dbos,
      Set<DBOSLifecycleListener> listenerSet,
      Map<String, RegisteredWorkflow> workflowMap,
      Map<String, RegisteredWorkflowInstance> instanceMap,
      List<Queue> queues,
      AlertHandler alertHandler) {

    if (isRunning.compareAndSet(false, true)) {
      logger.info("DBOS Executor starting");

      this.workflowMap = Collections.unmodifiableMap(workflowMap);
      this.instanceMap = Collections.unmodifiableMap(instanceMap);
      this.queues = Collections.unmodifiableList(queues);
      this.listeners = listenerSet;
      this.alertHandler = alertHandler;

      if (this.appVersion == null || this.appVersion.isEmpty()) {
        List<Class<?>> registeredClasses =
            workflowMap.values().stream()
                .map(wrapper -> wrapper.target().getClass())
                .collect(Collectors.toList());
        this.appVersion = AppVersionComputer.computeAppVersion(registeredClasses);
      }

      if (config.conductorKey() != null) {
        this.executorId = UUID.randomUUID().toString();
      }

      logger.info("System Database: {}", this.config.databaseUrl());
      logger.info("System Database User name: {}", this.config.dbUser());
      logger.info("Executor ID: {}", this.executorId);
      logger.info("Application Version: {}", this.appVersion);

      var executorServiceSupplier =
          new Supplier<ExecutorService>() {
            @Override
            public ExecutorService get() {
              try {
                // use virtual thread executor when available (i.e. Java 21+)
                var method = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
                return (ExecutorService) method.invoke(null);
              } catch (NoSuchMethodException
                  | IllegalAccessException
                  | InvocationTargetException e) {
                // fall back to fixed thread pool executor if virtual thread executor unavailable
                logger.debug("using newFixedThreadPool", e);
                int threadCount = Runtime.getRuntime().availableProcessors() * 50;
                return Executors.newFixedThreadPool(threadCount);
              }
            }
          };

      executorService = executorServiceSupplier.get();
      timeoutScheduler = Executors.newScheduledThreadPool(2);

      systemDatabase = SystemDatabase.create(config);
      systemDatabase.start();

      queueService = new QueueService(this, systemDatabase);
      queueService.start(queues, config.listenQueues());

      schedulerService = new SchedulerService(Constants.DBOS_INTERNAL_QUEUE);
      listeners.add(schedulerService);

      for (var listener : listeners) {
        listener.dbosLaunched();
      }

      var recoveryTask =
          new Runnable() {
            @Override
            public void run() {
              try {
                recoverPendingWorkflows(List.of(executorId()));
              } catch (Throwable t) {
                logger.error("Recovery task failed", t);
              }
            }
          };

      executorService.submit(recoveryTask);

      String conductorKey = config.conductorKey();
      if (conductorKey != null) {
        Conductor.Builder builder = new Conductor.Builder(this, systemDatabase, conductorKey);
        String domain = config.conductorDomain();
        if (domain != null && !domain.trim().isEmpty()) {
          builder.domain(domain);
        }
        conductor = builder.build();
        conductor.start();
      }

      if (config.adminServer()) {
        try {
          adminServer = new AdminServer(config.adminServerPort(), this, systemDatabase);
          adminServer.start();
        } catch (IOException e) {
          logger.error("DBOS Admin Server failed to start", e);
        }
      }

      logger.debug("DBOS Executor started");

    } else {
      logger.warn("DBOS Executor already started");
    }
  }

  @Override
  public void close() {
    if (isRunning.compareAndSet(true, false)) {
      logger.info("DBOS Executor stopping");

      if (adminServer != null) {
        adminServer.stop();
        adminServer = null;
      }

      if (conductor != null) {
        conductor.stop();
        conductor = null;
      }

      shutdownLifecycleListeners();

      queueService.stop();
      queueService = null;
      systemDatabase.close();
      systemDatabase = null;

      timeoutScheduler.shutdownNow();
      var notRun = executorService.shutdownNow();
      logger.debug("Shutting down DBOS Executor threadpool. Tasks not run {}", notRun.size());

      this.workflowMap = null;
      this.instanceMap = null;

      logger.debug("DBOS Executor stopped");
    }
  }

  public void deactivateLifecycleListeners() {
    if (isRunning.get()) {
      shutdownLifecycleListeners();
    }
  }

  private void shutdownLifecycleListeners() {
    for (var listener : listeners) {
      try {
        listener.dbosShutDown();
      } catch (Exception e) {
        logger.warn("Exception from shutdown", e);
      }
    }
  }

  // package private methods for test purposes
  boolean usingThreadPoolExecutor() {
    return this.executorService instanceof ThreadPoolExecutor;
  }

  SystemDatabase getSystemDatabase() {
    return systemDatabase;
  }

  QueueService getQueueService() {
    return queueService;
  }

  SchedulerService getSchedulerService() {
    return schedulerService;
  }

  public String appName() {
    return config.appName();
  }

  public String executorId() {
    return this.executorId;
  }

  public String appVersion() {
    return this.appVersion;
  }

  public String appId() {
    return this.appId;
  }

  public Collection<RegisteredWorkflow> getWorkflows() {
    return this.workflowMap.values();
  }

  public Collection<RegisteredWorkflowInstance> getInstances() {
    return this.instanceMap.values();
  }

  public List<Queue> getQueues() {
    return this.queues;
  }

  public Optional<Queue> getQueue(String queueName) {
    if (queues == null) {
      throw new IllegalStateException(
          "attempted to retrieve workflow from executor when DBOS not launched");
    }

    for (var queue : queues) {
      if (queue.name().equals(queueName)) {
        return Optional.of(queue);
      }
    }

    return Optional.empty();
  }

  public void fireAlertHandler(String name, String message, Map<String, String> metadata) {
    if (alertHandler != null) {
      alertHandler.invoke(name, message, metadata);
    } else {
      logger.warn(
          "No AlertHandler configured; dropping alert. name='{}', message='{}', metadata={}",
          name,
          message,
          metadata);
    }
  }

  WorkflowHandle<?, ?> recoverWorkflow(GetPendingWorkflowsOutput output) {
    Objects.requireNonNull(output, "output must not be null");
    String workflowId = Objects.requireNonNull(output.workflowId(), "workflowId must not be null");
    String queue = output.queueName();

    if (queue != null) {
      boolean cleared = systemDatabase.clearQueueAssignment(workflowId);
      if (cleared) {
        logger.debug("recoverWorkflow clear queue assignment {}", workflowId);
        return retrieveWorkflow(workflowId);
      }
    }

    logger.debug("recoverWorkflow execute {}", workflowId);
    return executeWorkflowById(workflowId, true, false);
  }

  public List<WorkflowHandle<?, ?>> recoverPendingWorkflows(List<String> executorIds) {
    Objects.requireNonNull(executorIds);
    String appVersion = appVersion();

    List<WorkflowHandle<?, ?>> handles = new ArrayList<>();
    for (String executorId : executorIds) {
      List<GetPendingWorkflowsOutput> pendingWorkflows;
      try {
        pendingWorkflows = systemDatabase.getPendingWorkflows(executorId, appVersion());
      } catch (Exception e) {
        logger.error(
            "getPendingWorkflows failed:  executor {}, application version {}",
            executorId,
            appVersion,
            e);
        return new ArrayList<>();
      }
      logger.info(
          "Recovering {} workflows for executor {} app version {}",
          pendingWorkflows.size(),
          executorId,
          appVersion);
      for (var output : pendingWorkflows) {
        try {
          handles.add(recoverWorkflow(output));
        } catch (Throwable t) {
          logger.error("Workflow {} recovery failed", output.workflowId(), t);
        }
      }
    }
    return handles;
  }

  private void postInvokeWorkflowResult(
      SystemDatabase systemDatabase, String workflowId, Object result, String serialization) {

    var serialized = SerializationUtil.serializeValue(result, serialization, this.serializer);
    systemDatabase.recordWorkflowOutput(workflowId, serialized.serializedValue());
  }

  private void postInvokeWorkflowError(
      SystemDatabase systemDatabase, String workflowId, Throwable error, String serialization) {

    var serialized = SerializationUtil.serializeError(error, serialization, this.serializer);
    systemDatabase.recordWorkflowError(workflowId, serialized.serializedValue());
  }

  /** This does not retry */
  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T callFunctionAsStep(
      ThrowingSupplier<T, E> fn, String functionName, String childWfId) throws E {
    DBOSContext ctx = DBOSContextHolder.get();

    int nextFuncId = 0;
    boolean inWorkflow = ctx != null && ctx.isInWorkflow();
    boolean inStep = ctx.isInStep();
    var startTime = System.currentTimeMillis();

    if (!inWorkflow || inStep) return fn.execute();

    nextFuncId = ctx.getAndIncrementFunctionId();

    StepResult result =
        systemDatabase.checkStepExecutionTxn(ctx.getWorkflowId(), nextFuncId, functionName);
    if (result != null) {
      return handleExistingResult(result, functionName);
    }

    T functionResult;

    try {
      functionResult = fn.execute();
    } catch (Exception e) {
      if (inWorkflow) {
        var jsonError = SerializationUtil.serializeError(e, null, this.serializer);
        StepResult r =
            new StepResult(
                ctx.getWorkflowId(),
                nextFuncId,
                functionName,
                null,
                jsonError.serializedValue(),
                childWfId,
                jsonError.serialization());
        systemDatabase.recordStepResultTxn(r, startTime);
      }
      throw (E) e;
    }

    // Record the successful result
    var toSaveSer = SerializationUtil.serializeValue(functionResult, null, this.serializer);
    StepResult o =
        new StepResult(
            ctx.getWorkflowId(),
            nextFuncId,
            functionName,
            toSaveSer.serializedValue(),
            null,
            childWfId,
            toSaveSer.serialization());
    systemDatabase.recordStepResultTxn(o, startTime);

    return functionResult;
  }

  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T runStepInternal(
      ThrowingSupplier<T, E> stepfunc, StepOptions opts, String childWfId) throws E {
    try {
      return runStepInternal(
          opts.name(),
          opts.retriesAllowed(),
          opts.maxAttempts(),
          opts.intervalSeconds(),
          opts.backOffRate(),
          childWfId,
          () -> {
            var res = stepfunc.execute();
            return res;
          });
    } catch (Exception t) {
      throw (E) t;
    }
  }

  @SuppressWarnings("unchecked")
  private <T, E extends Exception> T handleExistingResult(StepResult result, String functionName)
      throws E {
    if (result.output() != null) {
      Object outputValue =
          SerializationUtil.deserializeValue(result.output(), result.serialization(), this.serializer);
      return (T) outputValue;
    } else if (result.error() != null) {
      Throwable t =
          SerializationUtil.deserializeError(result.error(), result.serialization(), this.serializer);
      if (t instanceof Exception) {
        throw (E) t;
      } else {
        throw new RuntimeException(t.getMessage(), t);
      }
    } else {
      // Note that this shouldn't happen because the result is always wrapped in an
      // array, making
      // output not null.
      throw new IllegalStateException(
          String.format("Recorded output and error are both null for %s", functionName));
    }
  }

  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T runStepInternal(
      String stepName,
      boolean retryAllowed,
      int maxAttempts,
      double timeBetweenAttemptsSec,
      double backOffRate,
      String childWfId,
      ThrowingSupplier<T, E> function)
      throws E {
    if (maxAttempts < 1) {
      maxAttempts = 1;
    }
    if (!retryAllowed) {
      maxAttempts = 1;
    }
    DBOSContext ctx = DBOSContextHolder.get();
    boolean inWorkflow = ctx != null && ctx.isInWorkflow();

    if (!inWorkflow) {
      // if there is no workflow, execute the step function without checkpointing
      return function.execute();
    }

    String workflowId = ctx.getWorkflowId();
    var startTime = System.currentTimeMillis();

    logger.debug("Running step {} for workflow {}", stepName, workflowId);

    int stepFunctionId = ctx.getAndIncrementFunctionId();

    StepResult recordedResult =
        systemDatabase.checkStepExecutionTxn(workflowId, stepFunctionId, stepName);

    if (recordedResult != null) {
      String output = recordedResult.output();
      if (output != null) {
        Object outputValue =
            SerializationUtil.deserializeValue(output, recordedResult.serialization(), this.serializer);
        return (T) outputValue;
      }

      String error = recordedResult.error();
      if (error != null) {
        var throwable =
            SerializationUtil.deserializeError(error, recordedResult.serialization(), this.serializer);
        if (!(throwable instanceof Exception))
          throw new RuntimeException(throwable.getMessage(), throwable);
        throw (E) throwable;
      }
    }

    int currAttempts = 1;
    SerializationUtil.SerializedResult serializedOutput = null;
    Exception eThrown = null;
    T result = null;
    boolean shouldRetry = true;

    while (currAttempts <= maxAttempts) {
      try {
        ctx.setStepFunctionId(stepFunctionId);
        result = function.execute();
        shouldRetry = false;
        serializedOutput = SerializationUtil.serializeValue(result, null, this.serializer);
        eThrown = null;
      } catch (Exception e) {
        Throwable actual =
            (e instanceof InvocationTargetException)
                ? ((InvocationTargetException) e).getTargetException()
                : e;
        eThrown = e instanceof Exception ? (Exception) actual : e;
      } finally {
        ctx.resetStepFunctionId();
      }

      if (!shouldRetry || !retryAllowed) {
        break;
      }

      try {
        Thread.sleep((long) (timeBetweenAttemptsSec * 1000));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      timeBetweenAttemptsSec *= backOffRate;
      ++currAttempts;
    }

    if (eThrown == null) {
      StepResult stepResult =
          new StepResult(
              workflowId,
              stepFunctionId,
              stepName,
              serializedOutput.serializedValue(),
              null,
              childWfId,
              serializedOutput.serialization());
      systemDatabase.recordStepResultTxn(stepResult, startTime);
      return result;
    } else {
      var serError = SerializationUtil.serializeError(eThrown, null, this.serializer);
      StepResult stepResult =
          new StepResult(
              workflowId,
              stepFunctionId,
              stepName,
              null,
              serError.serializedValue(),
              childWfId,
              serError.serialization());
      systemDatabase.recordStepResultTxn(stepResult, startTime);
      throw (E) eThrown;
    }
  }

  /** Retrieve the workflowHandle for the workflowId */
  public <R, E extends Exception> WorkflowHandle<R, E> retrieveWorkflow(String workflowId) {
    logger.debug("retrieveWorkflow {}", workflowId);
    return new WorkflowHandleDBPoll<R, E>(workflowId);
  }

  public void sleep(Duration duration) {
    DBOSContext context = DBOSContextHolder.get();

    if (context.getWorkflowId() == null) {
      throw new IllegalStateException("sleep() must be called from within a workflow");
    }

    systemDatabase.sleep(context.getWorkflowId(), context.getAndIncrementFunctionId(), duration);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> resumeWorkflow(String workflowId) {
    // Execute the resume operation as a workflow step
    this.callFunctionAsStep(
        () -> {
          logger.info("Resuming workflow {}", workflowId);
          systemDatabase.resumeWorkflow(workflowId);
          return null; // void
        },
        "DBOS.resumeWorkflow",
        null);
    return retrieveWorkflow(workflowId);
  }

  public void cancelWorkflow(String workflowId) {

    // Execute the cancel operation as a workflow step
    this.callFunctionAsStep(
        () -> {
          logger.info("Cancelling workflow {}", workflowId);
          systemDatabase.cancelWorkflow(workflowId);
          return null; // void
        },
        "DBOS.cancelWorkflow",
        null);
  }

  public void deleteWorkflow(String workflowId, boolean deleteChildren) {
    Objects.requireNonNull(workflowId);
    this.callFunctionAsStep(
        () -> {
          logger.info(
              "Deleting workflow {}{}", workflowId, deleteChildren ? "" : " and its children");
          if (deleteChildren) {
            var children = systemDatabase.getWorkflowChildren(workflowId);
            var array =
                Stream.concat(Stream.of(workflowId), children.stream()).toArray(String[]::new);
            systemDatabase.deleteWorkflows(array);
          } else {
            systemDatabase.deleteWorkflows(workflowId);
          }

          return null;
        },
        "DBOS.deleteWorkflow",
        null);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> forkWorkflow(
      String workflowId, int startStep, ForkOptions options) {

    String forkedId =
        this.callFunctionAsStep(
            () -> {
              logger.info("Forking workflow:{} from step:{} ", workflowId, startStep);

              return systemDatabase.forkWorkflow(workflowId, startStep, options);
            },
            "DBOS.forkWorkflow",
            null);
    return retrieveWorkflow(forkedId);
  }

  public void globalTimeout(Long cutoff) {
    OffsetDateTime endTime = Instant.ofEpochMilli(cutoff).atOffset(ZoneOffset.UTC);
    globalTimeout(endTime);
  }

  public void globalTimeout(OffsetDateTime endTime) {
    ListWorkflowsInput pendingInput =
        new ListWorkflowsInput().withStatus(WorkflowState.PENDING).withEndTime(endTime);
    for (WorkflowStatus status : systemDatabase.listWorkflows(pendingInput)) {
      cancelWorkflow(status.workflowId());
    }

    ListWorkflowsInput enqueuedInput =
        new ListWorkflowsInput().withStatus(WorkflowState.ENQUEUED).withEndTime(endTime);
    for (WorkflowStatus status : systemDatabase.listWorkflows(enqueuedInput)) {
      cancelWorkflow(status.workflowId());
    }
  }

  public void send(
      String destinationId,
      Object message,
      String topic,
      InternalWorkflowsService internalWorkflowsService,
      String idempotencyKey,
      SerializationStrategy serialization) {

    DBOSContext ctx = DBOSContextHolder.get();
    if (ctx.isInStep()) {
      throw new IllegalStateException("DBOS.send() must not be called from within a step.");
    }
    if (!ctx.isInWorkflow()) {
      var sendWfid =
          idempotencyKey == null ? null : "%s-%s".formatted(destinationId, idempotencyKey);
      try (var wfid = new WorkflowOptions(sendWfid).setContext()) {
        internalWorkflowsService.sendWorkflow(destinationId, message, topic, serialization);
      }
      return;
    }

    if (idempotencyKey != null) {
      throw new IllegalArgumentException(
          "Invalid call to `DBOS.send` with an idempotency key from within a workflow");
    }
    int stepFunctionId = ctx.getAndIncrementFunctionId();

    systemDatabase.send(
        ctx.getWorkflowId(),
        stepFunctionId,
        destinationId,
        message,
        topic,
        serialization.formatName());
  }

  /**
   * Get a message sent to a particular topic
   *
   * @param topic the topic whose message to get
   * @param timeout duration to wait before the call times out
   * @return the message if there is one or else null
   */
  public Object recv(String topic, Duration timeout) {
    DBOSContext ctx = DBOSContextHolder.get();
    if (!ctx.isInWorkflow()) {
      throw new IllegalStateException("DBOS.recv() must be called from a workflow.");
    }
    if (ctx.isInStep()) {
      throw new IllegalStateException("DBOS.recv() must not be called from within a step.");
    }
    int stepFunctionId = ctx.getAndIncrementFunctionId();
    int timeoutFunctionId = ctx.getAndIncrementFunctionId();

    return systemDatabase.recv(
        ctx.getWorkflowId(), stepFunctionId, timeoutFunctionId, topic, timeout);
  }

  public void setEvent(String key, Object value, SerializationStrategy serialization) {
    logger.debug("Received setEvent for key {}", key);

    DBOSContext ctx = DBOSContextHolder.get();
    if (!ctx.isInWorkflow()) {
      throw new IllegalStateException("DBOS.setEvent() must be called from a workflow.");
    }

    if (serialization == null || serialization.equals(SerializationStrategy.DEFAULT)) {
      if (ctx.getSerialization() != null) {
        serialization = ctx.getSerialization();
      } else {
        serialization = SerializationStrategy.DEFAULT;
      }
    }

    var asStep = !ctx.isInStep();
    var stepId = ctx.isInStep() ? ctx.getCurrentFunctionId() : ctx.getAndIncrementFunctionId();
    systemDatabase.setEvent(
        ctx.getWorkflowId(), stepId, key, value, asStep, serialization.formatName());
  }

  public Object getEvent(String workflowId, String key, Duration timeout) {
    logger.debug("Received getEvent for {} {}", workflowId, key);

    DBOSContext ctx = DBOSContextHolder.get();

    if (ctx.isInWorkflow() && !ctx.isInStep()) {
      int stepFunctionId = ctx.getAndIncrementFunctionId();
      int timeoutFunctionId = ctx.getAndIncrementFunctionId();
      GetWorkflowEventContext callerCtx =
          new GetWorkflowEventContext(ctx.getWorkflowId(), stepFunctionId, timeoutFunctionId);
      return systemDatabase.getEvent(workflowId, key, timeout, callerCtx);
    }

    return systemDatabase.getEvent(workflowId, key, timeout, null);
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return this.callFunctionAsStep(
        () -> {
          return systemDatabase.listWorkflows(input);
        },
        "DBOS.listWorkflows",
        null);
  }

  public List<StepInfo> listWorkflowSteps(String workflowId) {
    return this.callFunctionAsStep(
        () -> {
          return systemDatabase.listWorkflowSteps(workflowId);
        },
        "DBOS.listWorkflowSteps",
        null);
  }

  public Optional<ExternalState> getExternalState(String service, String workflowName, String key) {
    return systemDatabase.getExternalState(service, workflowName, key);
  }

  public ExternalState upsertExternalState(ExternalState state) {
    return systemDatabase.upsertExternalState(state);
  }

  public WorkflowStatus getWorkflowStatus(String workflowId) {
    return this.callFunctionAsStep(
        () -> {
          return systemDatabase.getWorkflowStatus(workflowId);
        },
        "DBOS.getWorkflowStatus",
        null);
  }

  public <T, E extends Exception> T awaitWorkflowResult(String workflowId) throws E {
    var result = systemDatabase.<T>awaitWorkflowResult(workflowId);
    return Result.<T, E>process(result);
  }

  public <T, E extends Exception> T getResult(String workflowId) throws E {
    return this.callFunctionAsStep(
        () -> {
          return awaitWorkflowResult(workflowId);
        },
        "DBOS.getResult",
        workflowId);
  }

  public boolean patch(String patchName) {
    if (!config.enablePatching()) {
      throw new IllegalStateException("Patching must be enabled in DBOS Config");
    }

    DBOSContext ctx = DBOSContextHolder.get();
    if (ctx == null || !ctx.isInWorkflow()) {
      throw new IllegalStateException("DBOS.patch must be called from a workflow");
    }

    var workflowId = ctx.getWorkflowId();
    var functionId = ctx.getCurrentFunctionId();
    patchName = "DBOS.patch-%s".formatted(patchName);
    var patched = systemDatabase.patch(workflowId, functionId, patchName);
    if (patched) {
      ctx.getAndIncrementFunctionId();
    }
    return patched;
  }

  public boolean deprecatePatch(String patchName) {
    if (!config.enablePatching()) {
      throw new IllegalStateException("Patching must be enabled in DBOS Config");
    }

    DBOSContext ctx = DBOSContextHolder.get();
    if (ctx == null || !ctx.isInWorkflow()) {
      throw new IllegalStateException("DBOS.deprecatePatch must be called from a workflow");
    }

    var workflowId = ctx.getWorkflowId();
    var functionId = ctx.getCurrentFunctionId();
    patchName = "DBOS.patch-%s".formatted(patchName);
    var patchExists = systemDatabase.deprecatePatch(workflowId, functionId, patchName);
    if (patchExists) {
      ctx.getAndIncrementFunctionId();
    }
    return true;
  }

  private static <T, E extends Exception> Invocation captureInvocation(
      ThrowingSupplier<T, E> supplier) {
    AtomicReference<Invocation> capturedInvocation = new AtomicReference<>();
    DBOSInvocationHandler.hookHolder.set(
        (invocation) -> {
          if (!capturedInvocation.compareAndSet(null, invocation)) {
            throw new RuntimeException(
                "Only one @Workflow can be called in the startWorkflow lambda");
          }
        });

    try {
      supplier.execute();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      DBOSInvocationHandler.hookHolder.remove();
    }

    return Objects.requireNonNull(
        capturedInvocation.get(), "The startWorkflow lambda must call exactly one @Workflow");
  }

  private static WorkflowInfo getParent(DBOSContext ctx) {
    Objects.requireNonNull(ctx);
    if (ctx.isInWorkflow()) {
      if (ctx.isInStep()) {
        throw new IllegalStateException("cannot invoke a workflow from a step");
      }

      var workflowId = ctx.getWorkflowId();
      var functionId = ctx.getAndIncrementFunctionId();
      return new WorkflowInfo(workflowId, functionId);
    }
    return null;
  }

  private RegisteredWorkflow getWorkflow(Invocation inv) {
    return getWorkflow(inv.className(), inv.instanceName(), inv.workflowName());
  }

  private RegisteredWorkflow getWorkflow(
      String className, String instanceName, String workflowName) {
    var fqName = RegisteredWorkflow.fullyQualifiedName(className, instanceName, workflowName);
    var workflow = workflowMap.get(fqName);
    if (workflow == null) {
      throw new IllegalStateException("%s workflow not registered".formatted(fqName));
    }
    return workflow;
  }

  public record ExecutionOptions(
      String workflowId,
      Timeout timeout,
      Instant deadline,
      String queueName,
      String deduplicationId,
      Integer priority,
      String queuePartitionKey,
      boolean isRecoveryRequest,
      boolean isDequeuedRequest,
      String serialization) {
    public ExecutionOptions {
      if (timeout instanceof Timeout.Explicit explicit) {
        if (explicit.value().isNegative() || explicit.value().isZero()) {
          throw new IllegalArgumentException("timeout must be a positive non-zero duration");
        }
      }

      if (queuePartitionKey != null && queuePartitionKey.isEmpty()) {
        throw new IllegalArgumentException(
            "EnqueueOptions queuePartitionKey must not be empty if not null");
      }

      if (deduplicationId != null && deduplicationId.isEmpty()) {
        throw new IllegalArgumentException(
            "EnqueueOptions deduplicationId must not be empty if not null");
      }

      if (queuePartitionKey != null && queueName == null) {
        throw new IllegalArgumentException(
            "ExecutionOptions partition key provided but queue name is missing");
      }

      if (queuePartitionKey != null && deduplicationId != null) {
        throw new IllegalArgumentException(
            "ExecutionOptions partition key and deduplication ID cannot both be set");
      }
    }

    public ExecutionOptions(String workflowId, Duration timeout, Instant deadline) {
      this(workflowId, Timeout.of(timeout), deadline, null, null, null, null, false, false, null);
    }

    public ExecutionOptions asRecoveryRequest() {
      return new ExecutionOptions(
          this.workflowId,
          this.timeout,
          this.deadline,
          this.queueName,
          this.deduplicationId,
          this.priority,
          this.queuePartitionKey,
          true,
          false,
          this.serialization);
    }

    public ExecutionOptions asDequeuedRequest() {
      return new ExecutionOptions(
          this.workflowId,
          this.timeout,
          this.deadline,
          this.queueName,
          this.deduplicationId,
          this.priority,
          this.queuePartitionKey,
          false,
          true,
          this.serialization);
    }

    public ExecutionOptions withSerialization(String serialization) {
      return new ExecutionOptions(
          this.workflowId,
          this.timeout,
          this.deadline,
          this.queueName,
          this.deduplicationId,
          this.priority,
          this.queuePartitionKey,
          this.isRecoveryRequest,
          this.isDequeuedRequest,
          serialization);
    }

    public Duration timeoutDuration() {
      if (timeout instanceof Timeout.Explicit e) {
        return e.value();
      }
      return null;
    }

    @Override
    public String workflowId() {
      return workflowId != null && workflowId.isEmpty() ? null : workflowId;
    }
  }

  public <T, E extends Exception> WorkflowHandle<T, E> startWorkflow(
      RegisteredWorkflow regWorkflow, Object[] args, StartWorkflowOptions options) {
    var execOptions =
        new ExecutionOptions(
            options.workflowId(),
            options.timeout(),
            options.deadline(),
            options.queueName(),
            options.deduplicationId(),
            options.priority(),
            options.queuePartitionKey(),
            false,
            false,
            null);
    return executeWorkflow(regWorkflow, args, execOptions, null);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> startWorkflow(
      ThrowingSupplier<T, E> supplier, StartWorkflowOptions options) {

    logger.debug("startWorkflow {}", options);

    var invocation = captureInvocation(supplier);
    var workflow = getWorkflow(invocation);

    var ctx = DBOSContextHolder.get();
    var parent = getParent(ctx);
    var childWorkflowId =
        parent != null ? "%s-%d".formatted(parent.workflowId(), parent.functionId()) : null;

    var nextTimeout = options.timeout() != null ? options.timeout() : ctx.getNextTimeout();
    var nextDeadline = options.deadline() != null ? options.deadline() : ctx.getNextDeadline();

    // default to context timeout & deadline if nextTimeout is null or Inherit
    Duration timeout = ctx.getTimeout();
    Instant deadline = ctx.getDeadline();
    if (nextDeadline != null) {
      deadline = nextDeadline;
    } else if (nextTimeout instanceof Timeout.None) {
      // clear timeout and deadline to null if nextTimeout is None
      timeout = null;
      deadline = null;
    } else if (nextTimeout instanceof Timeout.Explicit e) {
      // set the timeout and deadline if nextTimeout is Explicit
      timeout = e.value();
      deadline = Instant.ofEpochMilli(System.currentTimeMillis() + e.value().toMillis());
    }

    var workflowId =
        Objects.requireNonNullElseGet(
            options.workflowId(),
            () ->
                Objects.requireNonNullElseGet(
                    ctx.getNextWorkflowId(childWorkflowId), () -> UUID.randomUUID().toString()));
    var execOptions =
        new ExecutionOptions(
            workflowId,
            Timeout.of(timeout),
            deadline,
            options.queueName(),
            options.deduplicationId(),
            options.priority(),
            options.queuePartitionKey(),
            false,
            false,
            null);
    return executeWorkflow(workflow, invocation.args(), execOptions, parent);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> invokeWorkflow(
      String className, String instanceName, String workflowName, Object[] args) {

    var fqName = RegisteredWorkflow.fullyQualifiedName(className, instanceName, workflowName);
    logger.debug("invokeWorkflow {}({})", fqName, args);

    var workflow = getWorkflow(className, instanceName, workflowName);

    var ctx = DBOSContextHolder.get();

    WorkflowInfo parent = getParent(ctx);
    String childWorkflowId =
        parent != null ? "%s-%d".formatted(parent.workflowId(), parent.functionId()) : null;

    var workflowId =
        Objects.requireNonNullElseGet(
            ctx.getNextWorkflowId(childWorkflowId), () -> UUID.randomUUID().toString());

    var nextTimeout = ctx.getNextTimeout();
    var nextDeadline = ctx.getNextDeadline();

    // default to context timeout & deadline if nextTimeout is null or Inherit
    Duration timeout = ctx.getTimeout();
    Instant deadline = ctx.getDeadline();
    if (nextDeadline != null) {
      deadline = nextDeadline;
    } else if (nextTimeout instanceof Timeout.None) {
      // clear timeout and deadline to null if nextTimeout is None
      timeout = null;
      deadline = null;
    } else if (nextTimeout instanceof Timeout.Explicit e) {
      // set the timeout and deadline if nextTimeout is Explicit
      timeout = e.value();
      deadline = Instant.ofEpochMilli(System.currentTimeMillis() + e.value().toMillis());
    }

    var options = new ExecutionOptions(workflowId, timeout, deadline);
    if (workflow.serializationStrategy() != null) {
      options = options.withSerialization(workflow.serializationStrategy().formatName());
    }
    return executeWorkflow(workflow, args, options, parent);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> executeWorkflowById(
      String workflowId, boolean isRecoveryRequest, boolean isDequeuedRequest) {
    logger.debug("executeWorkflowById {}", workflowId);

    var status = systemDatabase.getWorkflowStatus(workflowId);
    if (status == null) {
      logger.error("Workflow not found {}", workflowId);
      throw new DBOSNonExistentWorkflowException(workflowId);
    }

    Object[] inputs = status.input();
    var wfName =
        RegisteredWorkflow.fullyQualifiedName(
            status.className(), status.instanceName(), status.name());
    RegisteredWorkflow workflow = workflowMap.get(wfName);

    if (workflow == null) {
      throw new DBOSWorkflowFunctionNotFoundException(workflowId, wfName);
    }

    var options =
        new ExecutionOptions(workflowId, status.timeout(), status.deadline())
            .withSerialization(status.serialization());
    if (isRecoveryRequest) options = options.asRecoveryRequest();
    if (isDequeuedRequest) options = options.asDequeuedRequest();
    return executeWorkflow(workflow, inputs, options, null);
  }

  private <T, E extends Exception> WorkflowHandle<T, E> executeWorkflow(
      RegisteredWorkflow workflow, Object[] args, ExecutionOptions options, WorkflowInfo parent) {

    if (parent != null) {
      var childId = systemDatabase.checkChildWorkflow(parent.workflowId(), parent.functionId());
      if (childId.isPresent()) {
        return retrieveWorkflow(childId.get());
      }
    }

    if (options.serialization() == null) {
      if (workflow.serializationStrategy() != null) {
        options = options.withSerialization(workflow.serializationStrategy().formatName());
      }
    }

    Integer maxRetries = workflow.maxRecoveryAttempts() > 0 ? workflow.maxRecoveryAttempts() : null;

    final var foptions = options;

    if (options.queueName() != null) {

      var queue = queues.stream().filter(q -> q.name().equals(foptions.queueName())).findFirst();
      if (queue.isPresent()) {
        if (queue.get().partitionedEnabled() && options.queuePartitionKey() == null) {
          throw new IllegalArgumentException(
              "queue %s partitions enabled, but no partition key was provided"
                  .formatted(options.queueName()));
        }

        if (!queue.get().partitionedEnabled() && options.queuePartitionKey() != null) {
          throw new IllegalArgumentException(
              "queue %s is not a partitioned queue, but a partition key was provided"
                  .formatted(options.queueName()));
        }
      } else {
        throw new IllegalArgumentException(
            "queue %s does not exist".formatted(options.queueName()));
      }

      return enqueueWorkflow(
          workflow.name(),
          workflow.className(),
          workflow.instanceName(),
          maxRetries,
          args,
          options,
          parent,
          executorId(),
          appVersion(),
          appId(),
          systemDatabase,
          this.serializer);
    }

    logger.debug("executeWorkflow {}({}) {}", workflow.fullyQualifiedName(), args, options);

    var workflowId = Objects.requireNonNull(options.workflowId(), "workflowId must not be null");
    if (workflowId.isEmpty()) {
      throw new IllegalArgumentException("workflowId cannot be empty");
    }
    WorkflowInitResult initResult =
        preInvokeWorkflow(
            systemDatabase,
            workflow.name(),
            workflow.className(),
            workflow.instanceName(),
            maxRetries,
            args,
            workflowId,
            null,
            null,
            null,
            null,
            executorId(),
            appVersion(),
            appId(),
            parent,
            options.timeoutDuration(),
            options.deadline(),
            options.isRecoveryRequest,
            options.isDequeuedRequest,
            options.serialization(),
            this.serializer);
    if (!initResult.shouldExecuteOnThisExecutor()) {
      return retrieveWorkflow(workflowId);
    }
    if (initResult.status().equals(WorkflowState.SUCCESS.name())) {
      return retrieveWorkflow(workflowId);
    } else if (initResult.status().equals(WorkflowState.ERROR.name())) {
      logger.warn("Idempotency check not impl for error");
    } else if (initResult.status().equals(WorkflowState.CANCELLED.name())) {
      logger.warn("Idempotency check not impl for cancelled");
    }

    Supplier<T> task =
        () -> {
          DBOSContextHolder.clear();
          var res = workflowsInProgress.putIfAbsent(workflowId, true);
          if (res != null) throw new DBOSWorkflowExecutionConflictException(workflowId);
          try {
            logger.debug(
                "executeWorkflow task {}({}) {}", workflow.fullyQualifiedName(), args, foptions);

            DBOSContextHolder.set(
                new DBOSContext(
                    workflowId,
                    parent,
                    foptions.timeoutDuration(),
                    foptions.deadline(),
                    SerializationUtil.PORTABLE.equals(initResult.serialization())
                        ? SerializationStrategy.PORTABLE
                        : SerializationStrategy.DEFAULT));
            if (Thread.currentThread().isInterrupted()) {
              logger.debug("executeWorkflow task interrupted before workflow.invoke");
              return null;
            }
            T result = workflow.invoke(args);
            if (Thread.currentThread().isInterrupted()) {
              logger.debug("executeWorkflow task interrupted before postInvokeWorkflowResult");
              return null;
            }
            postInvokeWorkflowResult(
                systemDatabase, workflowId, result, initResult.serialization());
            return result;
          } catch (DBOSWorkflowExecutionConflictException e) {
            // don't persist execution conflict exception
            throw e;
          } catch (Exception e) {
            Throwable actual = e;

            while (true) {
              if (actual instanceof InvocationTargetException ite) {
                actual = ite.getTargetException();
              } else if (actual instanceof RuntimeException re && re.getCause() != null) {
                actual = re.getCause();
              } else {
                break;
              }
            }

            logger.error("executeWorkflow {}", actual);

            if (actual instanceof InterruptedException
                || actual instanceof DBOSWorkflowCancelledException) {
              throw new DBOSAwaitedWorkflowCancelledException(workflowId);
            }

            postInvokeWorkflowError(systemDatabase, workflowId, actual, initResult.serialization());
            throw e;
          } finally {
            DBOSContextHolder.clear();
            workflowsInProgress.remove(workflowId);
          }
        };

    long newTimeout = initResult.deadlineEpochMS() - System.currentTimeMillis();
    if (initResult.deadlineEpochMS() > 0 && newTimeout < 0) {
      systemDatabase.cancelWorkflow(workflowId);
      return retrieveWorkflow(workflowId);
    }

    var future = CompletableFuture.supplyAsync(task, executorService);
    if (newTimeout > 0) {
      timeoutScheduler.schedule(
          () -> {
            if (!future.isDone()) {
              systemDatabase.cancelWorkflow(workflowId);
              future.cancel(true);
            }
          },
          newTimeout,
          TimeUnit.MILLISECONDS);
    }

    return new WorkflowHandleFuture<T, E>(workflowId, future, this);
  }

  public static <T, E extends Exception> WorkflowHandle<T, E> enqueueWorkflow(
      String name,
      String className,
      String instanceName,
      Integer maxRetries,
      Object[] args,
      ExecutionOptions options,
      WorkflowInfo parent,
      String executorId,
      String appVersion,
      String appId,
      SystemDatabase systemDatabase,
      DBOSSerializer serializer) {

    logger.debug(
        "enqueueWorkflow {}({}) {}",
        RegisteredWorkflow.fullyQualifiedName(className, instanceName, name),
        args,
        options);

    var workflowId = Objects.requireNonNull(options.workflowId(), "workflowId must not be null");
    if (workflowId.isEmpty()) {
      throw new IllegalArgumentException("workflowId cannot be empty");
    }
    var queueName = Objects.requireNonNull(options.queueName(), "queueName must not be null");
    if (queueName.isEmpty()) {
      throw new IllegalArgumentException("queueName cannot be empty");
    }

    try {
      preInvokeWorkflow(
          systemDatabase,
          name,
          className,
          instanceName,
          maxRetries,
          args,
          workflowId,
          queueName,
          options.deduplicationId(),
          options.priority(),
          options.queuePartitionKey(),
          executorId,
          appVersion,
          appId,
          parent,
          options.timeoutDuration(),
          options.deadline(),
          options.isRecoveryRequest,
          options.isDequeuedRequest,
          options.serialization(),
          serializer);
      return new WorkflowHandleDBPoll<T, E>(workflowId);
    } catch (DBOSWorkflowExecutionConflictException e) {
      logger.debug("Workflow execution conflict for workflowId {}", workflowId);
      return new WorkflowHandleDBPoll<T, E>(workflowId);
    } catch (Throwable e) {
      var actual = (e instanceof InvocationTargetException ite) ? ite.getTargetException() : e;
      logger.error("enqueueWorkflow", actual);
      throw e;
    }
  }

  private static WorkflowInitResult preInvokeWorkflow(
      SystemDatabase systemDatabase,
      String workflowName,
      String className,
      String instanceName,
      Integer maxRetries,
      Object[] inputs,
      String workflowId,
      String queueName,
      String deduplicationId,
      Integer priority,
      String queuePartitionKey,
      String executorId,
      String appVersion,
      String appId,
      WorkflowInfo parentWorkflow,
      Duration timeout,
      Instant deadline,
      boolean isRecoveryRequest,
      boolean isDequeuedRequest,
      String serialization,
      DBOSSerializer serializer) {

    if (inputs == null) {
      inputs = new Object[0];
    }
    // Serialize inputs using the specified serialization format
    var serializedArgs = SerializationUtil.serializeArgs(inputs, null, serialization, serializer);
    String inputString = serializedArgs.serializedValue();
    String actualSerialization = serializedArgs.serialization();
    var startTime = System.currentTimeMillis();

    WorkflowState status = queueName == null ? WorkflowState.PENDING : WorkflowState.ENQUEUED;

    Long timeoutMs = timeout != null ? timeout.toMillis() : null;
    Long deadlineEpochMs =
        (queueName != null && timeoutMs != null)
            ? null
            : deadline != null ? deadline.toEpochMilli() : null;

    final int retries = maxRetries == null ? Constants.DEFAULT_MAX_RECOVERY_ATTEMPTS : maxRetries;
    WorkflowStatusInternal workflowStatusInternal =
        new WorkflowStatusInternal(
            workflowId,
            status,
            workflowName,
            className,
            instanceName,
            queueName,
            deduplicationId,
            priority == null ? 0 : priority,
            queuePartitionKey,
            null,
            null,
            null,
            inputString,
            null,
            null,
            executorId,
            appVersion,
            appId,
            null,
            null,
            null,
            null,
            timeoutMs,
            deadlineEpochMs,
            parentWorkflow != null ? parentWorkflow.workflowId() : null,
            actualSerialization);

    WorkflowInitResult[] initResult = {null};
    initResult[0] =
        systemDatabase.initWorkflowStatus(
            workflowStatusInternal, retries, isRecoveryRequest, isDequeuedRequest);

    if (parentWorkflow != null) {
      systemDatabase.recordChildWorkflow(
          parentWorkflow.workflowId(),
          workflowId,
          parentWorkflow.functionId(),
          workflowName,
          startTime);
    }

    return initResult[0];
  }
}
