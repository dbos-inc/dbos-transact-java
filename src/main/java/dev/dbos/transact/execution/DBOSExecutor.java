package dev.dbos.transact.execution;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.admin.AdminServer;
import dev.dbos.transact.conductor.Conductor;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.context.WorkflowInfo;
import dev.dbos.transact.database.DbRetry;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.database.GetWorkflowEventContext;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.database.WorkflowInitResult;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.internal.AppVersionComputer;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.tempworkflows.InternalWorkflowsService;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
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
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSExecutor implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(DBOSExecutor.class);

  private final DBOSConfig config;

  private String appVersion;
  private String executorId;

  private Map<String, RegisteredWorkflow> workflowMap;
  private List<Queue> queues;

  private SystemDatabase systemDatabase;
  private QueueService queueService;
  private SchedulerService schedulerService;
  private RecoveryService recoveryService;
  private AdminServer adminServer;
  private Conductor conductor;
  private ExecutorService executorService;
  private ScheduledExecutorService timeoutScheduler;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  public DBOSExecutor(DBOSConfig config) {
    this.config = config;
  }

  public void start(
      DBOS.Instance dbos, Map<String, RegisteredWorkflow> workflowMap, List<Queue> queues) {

    if (isRunning.compareAndSet(false, true)) {
      this.workflowMap = Collections.unmodifiableMap(workflowMap);
      this.queues = Collections.unmodifiableList(queues);

      this.executorId = System.getenv("DBOS__VMID");
      if (this.executorId == null || this.executorId.isEmpty()) {
        this.executorId = config.executorId();
      }
      if (this.executorId == null || this.executorId.isEmpty()) {
        this.executorId = config.conductorKey() == null ? "local" : UUID.randomUUID().toString();
      }

      this.appVersion = System.getenv("DBOS__APPVERSION");
      if (this.appVersion == null || this.appVersion.isEmpty()) {
        this.appVersion = config.appVersion();
      }

      if (this.appVersion == null || this.appVersion.isEmpty()) {
        List<Class<?>> registeredClasses =
            workflowMap.values().stream()
                .map(wrapper -> wrapper.target().getClass())
                .collect(Collectors.toList());
        this.appVersion = AppVersionComputer.computeAppVersion(registeredClasses);
      }

      logger.info("Executor ID: {}", this.executorId);
      logger.info("Application Version: {}", this.appVersion);

      executorService = Executors.newCachedThreadPool();
      timeoutScheduler = Executors.newScheduledThreadPool(2);

      systemDatabase = new SystemDatabase(config);
      systemDatabase.start();

      queueService = new QueueService(this, systemDatabase);
      queueService.start(queues);

      Queue schedulerQueue = null;
      for (var queue : queues) {
        if (queue.name().equals(Constants.DBOS_SCHEDULER_QUEUE)) {
          schedulerQueue = queue;
        }
      }
      schedulerService = new SchedulerService(this, schedulerQueue);
      schedulerService.start();

      recoveryService = new RecoveryService(this, systemDatabase);
      recoveryService.start();

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

      logger.info("DBOS started");

    } else {
      logger.warn("DBOS Executor already started");
    }
  }

  @Override
  public void close() {
    if (isRunning.compareAndSet(true, false)) {

      if (adminServer != null) {
        adminServer.stop();
        adminServer = null;
      }

      if (conductor != null) {
        conductor.stop();
        conductor = null;
      }

      recoveryService.stop();
      recoveryService = null;
      schedulerService.stop();
      schedulerService = null;
      queueService.stop();
      queueService = null;
      systemDatabase.stop();
      systemDatabase = null;

      timeoutScheduler.shutdownNow();
      executorService.shutdownNow();

      this.workflowMap = null;
    }
  }

  // package private method for test purposes
  SystemDatabase getSystemDatabase() {
    return systemDatabase;
  }

  // package private method for test purposes
  QueueService getQueueService() {
    return queueService;
  }

  // package private method for test purposes
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

  public RegisteredWorkflow getWorkflow(
      String className, String instanceName, String workflowName) {
    return workflowMap.get(
        RegisteredWorkflow.fullyQualifiedWFName(className, instanceName, workflowName));
  }

  public Collection<RegisteredWorkflow> getWorkflows() {
    return this.workflowMap.values();
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

  WorkflowHandle<?, ?> recoverWorkflow(GetPendingWorkflowsOutput output) {
    Objects.requireNonNull(output, "output must not be null");
    String workflowId = output.getWorkflowUuid();
    Objects.requireNonNull(workflowId, "workflowId must not be null");
    String queue = output.getQueueName();

    logger.debug("Recovery executing workflow {}", workflowId);

    if (queue != null) {
      boolean cleared = systemDatabase.clearQueueAssignment(workflowId);
      if (cleared) {
        return retrieveWorkflow(workflowId);
      }
    }
    return executeWorkflowById(workflowId);
  }

  public List<WorkflowHandle<?, ?>> recoverPendingWorkflows(List<String> executorIDs) {
    if (executorIDs == null) {
      executorIDs = new ArrayList<>(List.of("local"));
    }

    String appVersion = appVersion();

    List<WorkflowHandle<?, ?>> handles = new ArrayList<>();
    for (String executorId : executorIDs) {
      List<GetPendingWorkflowsOutput> pendingWorkflows;
      try {
        pendingWorkflows = systemDatabase.getPendingWorkflows(executorId, appVersion);
      } catch (Exception e) {
        logger.error(
            "Failed to get pending workflows for executor {} and application version {}",
            executorId,
            appVersion,
            e);
        return new ArrayList<>();
      }
      logger.debug(
          "Recovering {} workflow(s) for executor {} and application version {}",
          pendingWorkflows.size(),
          executorId,
          appVersion);
      for (GetPendingWorkflowsOutput output : pendingWorkflows) {
        try {
          handles.add(recoverWorkflow(output));
        } catch (Exception e) {
          logger.warn("Recovery of workflow {} failed", output.getWorkflowUuid(), e);
        }
      }
    }
    return handles;
  }

  private static void postInvokeWorkflowResult(
      SystemDatabase systemDatabase, String workflowId, Object result) {

    String resultString = JSONUtil.serialize(result);
    systemDatabase.recordWorkflowOutput(workflowId, resultString);
  }

  private static void postInvokeWorkflowError(
      SystemDatabase systemDatabase, String workflowId, Throwable error) {

    String errorString = JSONUtil.serializeAppException(error);

    systemDatabase.recordWorkflowError(workflowId, errorString);
  }

  /** This does not retry */
  public <T, E extends Exception> T callFunctionAsStep(
      ThrowingSupplier<T, E> fn, String functionName, String childWfId) throws E {
    DBOSContext ctx = DBOSContextHolder.get();

    int nextFuncId = 0;
    boolean inWorkflow = ctx != null && ctx.isInWorkflow();
    boolean inStep = ctx.isInStep();

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
        String jsonError = JSONUtil.serializeAppException(e);
        StepResult r =
            new StepResult(
                ctx.getWorkflowId(), nextFuncId, functionName, null, jsonError, childWfId);
        systemDatabase.recordStepResultTxn(r);
      }
      throw (E) e;
    }

    // Record the successful result
    String jsonOutput = JSONUtil.serialize(functionResult);
    StepResult o =
        new StepResult(ctx.getWorkflowId(), nextFuncId, functionName, jsonOutput, null, childWfId);
    systemDatabase.recordStepResultTxn(o);

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
          opts.backOffRate(),
          opts.intervalSeconds(),
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
    if (result.getOutput() != null) {
      Object[] resArray = JSONUtil.deserializeToArray(result.getOutput());
      return resArray == null ? null : (T) resArray[0];
    } else if (result.getError() != null) {
      Throwable t = JSONUtil.deserializeAppException(result.getError());
      if (t instanceof Exception) {
        throw (E) t;
      } else {
        throw new RuntimeException(t.getMessage(), t);
      }
    } else {
      // Note that this shouldn't happen because the result is always wrapped in an array, making
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

    logger.debug("Running step {} for workflow {}", stepName, workflowId);

    int stepFunctionId = ctx.getAndIncrementFunctionId();

    StepResult recordedResult =
        systemDatabase.checkStepExecutionTxn(workflowId, stepFunctionId, stepName);

    if (recordedResult != null) {
      String output = recordedResult.getOutput();
      if (output != null) {
        Object[] stepO = JSONUtil.deserializeToArray(output);
        return stepO == null ? null : (T) stepO[0];
      }

      String error = recordedResult.getError();
      if (error != null) {
        var throwable = JSONUtil.deserializeAppException(error);
        if (!(throwable instanceof Exception))
          throw new RuntimeException(throwable.getMessage(), throwable);
        throw (E) throwable;
      }
    }

    int currAttempts = 1;
    String serializedOutput = null;
    Exception eThrown = null;
    T result = null;
    boolean shouldRetry = true;

    while (currAttempts <= maxAttempts) {
      try {
        ctx.setStepFunctionId(stepFunctionId);
        result = function.execute();
        shouldRetry = false;
        serializedOutput = JSONUtil.serialize(result);
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
          new StepResult(workflowId, stepFunctionId, stepName, serializedOutput, null, childWfId);
      systemDatabase.recordStepResultTxn(stepResult);
      return result;
    } else {
      StepResult stepResult =
          new StepResult(
              workflowId,
              stepFunctionId,
              stepName,
              null,
              JSONUtil.serializeAppException(eThrown),
              childWfId);
      systemDatabase.recordStepResultTxn(stepResult);
      throw (E) eThrown;
    }
  }

  /** Retrieve the workflowHandle for the workflowId */
  public <R, E extends Exception> WorkflowHandle<R, E> retrieveWorkflow(String workflowId) {
    logger.debug("retrieveWorkflow {}", workflowId);
    return retrieveWorkflow(workflowId, systemDatabase);
  }

  private static <R, E extends Exception> WorkflowHandle<R, E> retrieveWorkflow(
      String workflowId, SystemDatabase systemDatabase) {
    return new WorkflowHandleDBPoll<R, E>(workflowId);
  }

  public void sleep(Duration duration) {
    // CB TODO: This should be OK outside DBOS

    DBOSContext context = DBOSContextHolder.get();

    if (context.getWorkflowId() == null) {
      throw new IllegalStateException("sleep() must be called from within a workflow");
    }

    systemDatabase.sleep(
        context.getWorkflowId(), context.getAndIncrementFunctionId(), duration, false);
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
      InternalWorkflowsService internalWorkflowsService) {

    DBOSContext ctx = DBOSContextHolder.get();
    if (ctx.isInStep()) {
      throw new IllegalStateException("DBOS.send() must not be called from within a step.");
    }
    if (!ctx.isInWorkflow()) {
      internalWorkflowsService.sendWorkflow(destinationId, message, topic);
      return;
    }
    int stepFunctionId = ctx.getAndIncrementFunctionId();

    systemDatabase.send(ctx.getWorkflowId(), stepFunctionId, destinationId, message, topic);
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

  public void setEvent(String key, Object value) {
    logger.debug("Received setEvent for key {}", key);

    DBOSContext ctx = DBOSContextHolder.get();
    if (!ctx.isInWorkflow()) {
      throw new IllegalStateException("DBOS.setEvent() must be called from a workflow.");
    }
    if (!ctx.isInStep()) {
      int stepFunctionId = ctx.getAndIncrementFunctionId();
      systemDatabase.setEvent(ctx.getWorkflowId(), stepFunctionId, key, value);
    } else {
      systemDatabase.setEvent(ctx.getWorkflowId(), null, key, value);
    }
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
    return systemDatabase.<T, E>awaitWorkflowResult(workflowId);
  }

  public <T, E extends Exception> T getResult(String workflowId) throws E {
    return this.callFunctionAsStep(
        () -> {
          return awaitWorkflowResult(workflowId);
        },
        "DBOS.getResult",
        workflowId);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> startWorkflow(
      ThrowingSupplier<T, E> supplier, StartWorkflowOptions options) {

    var ctx = DBOSContextHolder.get();
    Integer functionId = null;

    if (ctx.isInWorkflow()) {
      if (ctx.isInStep()) {
        throw new IllegalStateException("cannot invoke a workflow from a step");
      }
      functionId = ctx.getAndIncrementFunctionId();
    }

    if (options.workflowId() == null) {
      options = options.withWorkflowId(ctx.getNextWorkflowId());
    }

    CompletableFuture<String> future = new CompletableFuture<>();
    var newCtx = new DBOSContext(ctx, options, functionId, future);

    Callable<T> task =
        () -> {
          DBOSContextHolder.clear();
          try {
            DBOSContextHolder.set(newCtx);
            return supplier.execute();
          } finally {
            DBOSContextHolder.clear();
          }
        };

    executorService.submit(task);
    try {
      var wfid = future.get(10, TimeUnit.SECONDS);
      return retrieveWorkflow(wfid);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("startWorkflow future await interupted", e);
    } catch (TimeoutException e) {
      throw new RuntimeException("startWorkflow future await timed out", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("startWorkflow future execution exception", e);
    }
  }

  public <T, E extends Exception> WorkflowHandle<T, E> invokeWorkflow(
      String clsName, String instName, String wfName, Object[] args) {
    var workflow = getWorkflow(clsName, instName, wfName);
    if (workflow == null) {
      throw new IllegalStateException(
          "%s/%s/%s workflow not registered".formatted(clsName, instName, wfName));
    }

    var ctx = DBOSContextHolder.get();
    if (!ctx.validateStartedWorkflow()) {
      logger.error(
          "Attempting to call {} workflow from a startWorkflow lambda that has already invoked a workflow",
          wfName);
      throw new IllegalCallerException();
    }

    WorkflowInfo parent = null;
    String childWorkflowId = null;

    if (ctx.isInWorkflow()) {
      if (ctx.isInStep()) {
        throw new IllegalStateException("cannot invoke a workflow from a step");
      }

      var workflowId = ctx.getWorkflowId();
      var functionId = ctx.getAndIncrementFunctionId();
      parent = new WorkflowInfo(workflowId, functionId);

      childWorkflowId = "%s-%d".formatted(ctx.getWorkflowId(), functionId);
    }

    var workflowId =
        Objects.requireNonNullElseGet(
            ctx.getNextWorkflowId(childWorkflowId), () -> UUID.randomUUID().toString());

    var nextTimeout = ctx.getNextTimeout();

    // default to context timeout & deadline if nextTimeout is null or Inherit
    Duration timeout = ctx.getTimeout();
    Instant deadline = ctx.getDeadline();
    if (nextTimeout instanceof Timeout.None) {
      // clear timeout and deadline to null if nextTimeout is None
      timeout = null;
      deadline = null;
    } else if (nextTimeout instanceof Timeout.Explicit e) {
      // set the timeout and deadline if nextTimeout is Explicit
      timeout = e.value();
      deadline = Instant.ofEpochMilli(System.currentTimeMillis() + e.value().toMillis());
    }

    try {
      var options =
          new ExecuteWorkflowOptions(
              workflowId,
              timeout,
              deadline,
              ctx.getQueueName(),
              ctx.getDeduplicationId(),
              ctx.getPriority());
      return executeWorkflow(workflow, args, options, parent, ctx.getStartWorkflowFuture());
    } finally {
      ctx.setStartedWorkflowId(workflowId);
    }
  }

  public <T, E extends Exception> WorkflowHandle<T, E> executeWorkflowById(String workflowId) {
    logger.debug("executeWorkflowById {}", workflowId);

    var status = systemDatabase.getWorkflowStatus(workflowId);
    if (status == null) {
      logger.error("Workflow not found {}", workflowId);
      throw new DBOSNonExistentWorkflowException(workflowId);
    }

    Object[] inputs = status.input();
    var wfName =
        RegisteredWorkflow.fullyQualifiedWFName(
            status.className(), status.instanceName(), status.name());
    RegisteredWorkflow workflow = workflowMap.get(wfName);

    if (workflow == null) {
      throw new DBOSWorkflowFunctionNotFoundException(workflowId, wfName);
    }

    var options = new ExecuteWorkflowOptions(workflowId, status.getTimeout(), status.getDeadline());
    return executeWorkflow(workflow, inputs, options, null, null);
  }

  public record ExecuteWorkflowOptions(
      String workflowId,
      Duration timeout,
      Instant deadline,
      String queueName,
      String deduplicationId,
      OptionalInt priority) {

    public ExecuteWorkflowOptions {
      if (Objects.requireNonNull(workflowId, "workflowId must not be null").isEmpty()) {
        throw new IllegalArgumentException("workflowId must not be empty");
      }

      if (timeout != null && timeout.isNegative()) {
        throw new IllegalStateException("negative timeout");
      }
    }

    public ExecuteWorkflowOptions(String workflowId, Duration timeout, Instant deadline) {
      this(workflowId, timeout, deadline, null, null, null);
    }

    public long getTimeoutMillis() {
      return Objects.requireNonNullElse(timeout, Duration.ZERO).toMillis();
    }
  }

  public <T, E extends Exception> WorkflowHandle<T, E> executeWorkflow(
      RegisteredWorkflow workflow,
      Object[] args,
      ExecuteWorkflowOptions options,
      WorkflowInfo parent,
      CompletableFuture<String> latch) {

    if (options.queueName != null) {
      return enqueueWorkflow(
          workflow.name(),
          workflow.className(),
          workflow.instanceName(),
          args,
          options,
          parent,
          executorId(),
          appVersion(),
          systemDatabase,
          latch);
    }

    var workflowId = options.workflowId();
    WorkflowInitResult initResult = null;
    try {
      if (parent != null) {
        var childId = systemDatabase.checkChildWorkflow(parent.workflowId(), parent.functionId());
        if (childId.isPresent()) {
          return retrieveWorkflow(childId.get());
        }
      }

      initResult =
          preInvokeWorkflow(
              systemDatabase,
              workflow.name(),
              workflow.className(),
              workflow.instanceName(),
              args,
              workflowId,
              null,
              null,
              OptionalInt.empty(),
              executorId(),
              appVersion(),
              parent,
              options.timeout(),
              options.deadline());
      if (initResult.getStatus().equals(WorkflowState.SUCCESS.name())) {
        return retrieveWorkflow(workflowId);
      } else if (initResult.getStatus().equals(WorkflowState.ERROR.name())) {
        logger.warn("Idempotency check not impl for error");
      } else if (initResult.getStatus().equals(WorkflowState.CANCELLED.name())) {
        logger.warn("Idempotency check not impl for cancelled");
      }
    } catch (Exception e) {
      if (latch != null) {
        latch.completeExceptionally(e);
      }
      throw e;
    } finally {
      if (latch != null) {
        latch.complete(options.workflowId);
      }
    }

    Callable<T> task =
        () -> {
          DBOSContextHolder.clear();
          try {
            logger.debug(
                "executeWorkflow task {} {}",
                Objects.requireNonNullElse(options.timeout, Duration.ZERO).toMillis(),
                Objects.requireNonNullElse(options.deadline, Instant.EPOCH).toEpochMilli());

            DBOSContextHolder.set(
                new DBOSContext(workflowId, parent, options.timeout, options.deadline));
            T result = workflow.invoke(args);
            postInvokeWorkflowResult(systemDatabase, workflowId, result);
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

            postInvokeWorkflowError(systemDatabase, workflowId, actual);
            throw e;
          } finally {
            DBOSContextHolder.clear();
          }
        };

    long newTimeout = initResult.getDeadlineEpochMS() - System.currentTimeMillis();
    if (initResult.getDeadlineEpochMS() > 0 && newTimeout < 0) {
      systemDatabase.cancelWorkflow(workflowId);
      return retrieveWorkflow(workflowId);
    }

    var future = executorService.submit(task);
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
      Object[] args,
      ExecuteWorkflowOptions options,
      WorkflowInfo parent,
      String executorId,
      String appVersion,
      SystemDatabase systemDatabase,
      CompletableFuture<String> latch) {
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
          args,
          workflowId,
          queueName,
          options.deduplicationId(),
          options.priority(),
          executorId,
          appVersion,
          parent,
          options.timeout(),
          options.deadline());
      return retrieveWorkflow(workflowId, systemDatabase);
    } catch (Throwable e) {
      var actual = (e instanceof InvocationTargetException ite) ? ite.getTargetException() : e;
      logger.error("enqueueWorkflow", actual);
      if (latch != null) {
        latch.completeExceptionally(actual);
      }
      throw e;
    } finally {
      if (latch != null) {
        latch.complete(workflowId);
      }
    }
  }

  private static WorkflowInitResult preInvokeWorkflow(
      SystemDatabase systemDatabase,
      String workflowName,
      String className,
      String instanceName,
      Object[] inputs,
      String workflowId,
      String queueName,
      String deduplicationId,
      OptionalInt priority,
      String executorId,
      String appVersion,
      WorkflowInfo parentWorkflow,
      Duration timeout,
      Instant deadline) {

    if (inputs == null) {
      inputs = new Object[0];
    }
    String inputString = JSONUtil.serializeArray(inputs);

    WorkflowState status = queueName == null ? WorkflowState.PENDING : WorkflowState.ENQUEUED;

    Long timeoutMs = timeout != null ? timeout.toMillis() : null;
    Long deadlineEpochMs =
        queueName != null ? null : deadline != null ? deadline.toEpochMilli() : null;

    WorkflowStatusInternal workflowStatusInternal = 
        new WorkflowStatusInternal(
            workflowId,
            status,
            workflowName,
            className,
            instanceName,
            queueName,
            deduplicationId,
            priority.orElse(0),
            null, null, null,
            inputString, null, null,
            executorId, appVersion, null,
            null, null, null,
            null, timeoutMs, deadlineEpochMs);

    WorkflowInitResult[] initResult = {null};
    DbRetry.run(
        () -> {
          initResult[0] = systemDatabase.initWorkflowStatus(workflowStatusInternal, 3);
        });

    if (parentWorkflow != null) {
      systemDatabase.recordChildWorkflow(
          parentWorkflow.workflowId(), workflowId, parentWorkflow.functionId(), workflowName);
    }

    return initResult[0];
  }
}
