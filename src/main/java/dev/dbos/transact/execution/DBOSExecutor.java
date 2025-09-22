package dev.dbos.transact.execution;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.conductor.Conductor;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.GetWorkflowEventContext;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.database.WorkflowInitResult;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.http.HttpServer;
import dev.dbos.transact.http.controllers.AdminController;
import dev.dbos.transact.internal.AppVersionComputer;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.queue.ListQueuedWorkflowsInput;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.queue.QueueService;
import dev.dbos.transact.scheduled.SchedulerService;
import dev.dbos.transact.scheduled.SchedulerService.ScheduledInstance;
import dev.dbos.transact.tempworkflows.InternalWorkflowsService;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.StepOptions;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;
import dev.dbos.transact.workflow.internal.WorkflowHandleFuture;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSExecutor implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(DBOSExecutor.class);

  private final DBOSConfig config;

  private DBOS dbos;
  private String appVersion;
  private String executorId;

  private Map<String, RegisteredWorkflow> workflowMap;
  private List<Queue> queues;

  private SystemDatabase systemDatabase;
  private QueueService queueService;
  private SchedulerService schedulerService;
  private RecoveryService recoveryService;
  private HttpServer httpServer;
  private Conductor conductor;
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(2);
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  public DBOSExecutor(DBOSConfig config) {
    this.config = config;
  }

  public void start(
      DBOS dbos,
      Map<String, RegisteredWorkflow> workflowMap,
      List<Queue> queues,
      List<ScheduledInstance> scheduledWorkflows) {

    if (isRunning.compareAndSet(false, true)) {
      this.dbos = dbos;
      this.workflowMap = workflowMap;
      this.queues = queues;

      this.executorId = System.getenv("DBOS__VMID");
      if (this.executorId == null) {
        this.executorId = "local";
      }

      this.appVersion = System.getenv("DBOS__APPVERSION");
      if (this.appVersion == null) {
        List<Class<?>> registeredClasses =
            workflowMap.values().stream()
                .map(wrapper -> wrapper.target().getClass())
                .collect(Collectors.toList());
        this.appVersion = AppVersionComputer.computeAppVersion(registeredClasses);
      }

      systemDatabase = new SystemDatabase(config);
      systemDatabase.start();

      queueService = new QueueService(this, systemDatabase);
      queueService.start(queues);

      Queue schedulerQueue = null;
      for (var queue : queues) {
        if (queue.getName() == Constants.DBOS_SCHEDULER_QUEUE) {
          schedulerQueue = queue;
        }
      }
      schedulerService = new SchedulerService(this, schedulerQueue, scheduledWorkflows);
      schedulerService.start();

      recoveryService = new RecoveryService(this, systemDatabase);
      recoveryService.start();

      String conductorKey = config.getConductorKey();
      if (conductorKey != null) {
        Conductor.Builder builder = new Conductor.Builder(this, systemDatabase, conductorKey);
        String domain = config.getConductorDomain();
        if (domain != null && !domain.trim().isEmpty()) {
          builder.domain(domain);
        }
        conductor = builder.build();
        conductor.start();
      }

      if (config.isHttp()) {
        httpServer =
            HttpServer.getInstance(
                config.getHttpPort(), new AdminController(this, systemDatabase, queues));
        if (config.isHttpAwaitOnStart()) {
          Thread httpThread =
              new Thread(
                  () -> {
                    logger.info("Start http in background thread");
                    httpServer.startAndBlock();
                  },
                  "http-server-thread");
          httpThread.setDaemon(false); // Keep process alive
          httpThread.start();
        } else {
          httpServer.start();
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (isRunning.compareAndSet(true, false)) {

      if (httpServer != null) {
        httpServer.stop();
        httpServer = null;
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

      this.workflowMap = null;
      this.dbos = null;
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

  public String getAppName() {
    return config.getName();
  }

  public String getExecutorId() {
    return this.executorId;
  }

  public String getAppVersion() {
    return this.appVersion;
  }

  public RegisteredWorkflow getWorkflow(String workflowName) {
    if (workflowMap == null) {
      throw new IllegalStateException(
          "attempted to retrieve workflow from executor when DBOS not launched");
    }

    return workflowMap.get(workflowName);
  }

  public Optional<Queue> getQueue(String queueName) {
    if (queues == null) {
      throw new IllegalStateException(
          "attempted to retrieve workflow from executor when DBOS not launched");
    }

    for (var queue : queues) {
      if (queue.getName() == queueName) {
        return Optional.of(queue);
      }
    }

    return Optional.empty();
  }

  WorkflowHandle<?, ?> recoverWorkflow(GetPendingWorkflowsOutput output) throws Exception {
    Objects.requireNonNull(output);
    String workflowId = output.getWorkflowUuid();
    Objects.requireNonNull(workflowId);
    String queue = output.getQueueName();

    logger.info("Recovery executing workflow {}", workflowId);

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

    String appVersion = getAppVersion();

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
      logger.info(
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

  record ParentWorkflow(String workflowId, int functionId) {
    public static ParentWorkflow fromContext() {
      DBOSContext ctx = DBOSContextHolder.get();
      return ctx.hasParent()
          ? new ParentWorkflow(ctx.getParentWorkflowId(), ctx.getParentFunctionId())
          : null;
    }

    public static ParentWorkflow fromContext(DBOSContext ctx) {
      return ctx.hasParent()
          ? new ParentWorkflow(ctx.getParentWorkflowId(), ctx.getParentFunctionId())
          : null;
    }
  }

  private static WorkflowInitResult preInvokeWorkflow(
      SystemDatabase systemDatabase,
      String workflowName,
      String className,
      Object[] inputs,
      String workflowId,
      String queueName,
      String executorId,
      String appVersion,
      ParentWorkflow parentWorkflow,
      long workflowTimeoutMs) {

    // TODO: queue deduplication and priority

    String inputString = JSONUtil.serializeArray(inputs);

    WorkflowState status = queueName == null ? WorkflowState.PENDING : WorkflowState.ENQUEUED;

    long workflowDeadlineEpoch = 0;
    if (workflowTimeoutMs > 0) {
      workflowDeadlineEpoch = System.currentTimeMillis() + workflowTimeoutMs;
    }

    WorkflowStatusInternal workflowStatusInternal =
        new WorkflowStatusInternal(
            workflowId,
            status,
            workflowName,
            className,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            queueName,
            executorId,
            appVersion,
            null,
            0,
            workflowTimeoutMs,
            workflowDeadlineEpoch,
            null,
            1,
            inputString);

    WorkflowInitResult initResult = null;
    try {
      initResult = systemDatabase.initWorkflowStatus(workflowStatusInternal, 3);
    } catch (Exception e) {
      logger.error("Error inserting into workflow_status", e);
      throw new DBOSException(UNEXPECTED.getCode(), e.getMessage(), e);
    }

    if (parentWorkflow != null) {
      systemDatabase.recordChildWorkflow(
          parentWorkflow.workflowId, workflowId, parentWorkflow.functionId, workflowName);
    }

    return initResult;
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

  private <T> T runWorkflowAndSaveResult(
      Object target, Object[] args, WorkflowFunctionReflect function, String workflowId)
      throws Exception {

    try {

      @SuppressWarnings("unchecked")
      T result = (T) function.invoke(target, args);

      postInvokeWorkflowResult(systemDatabase, workflowId, result);
      return result;
    } catch (Exception e) {
      Throwable actual =
          (e instanceof InvocationTargetException)
              ? ((InvocationTargetException) e).getTargetException()
              : e;

      logger.error("Error in runWorkflow", actual);

      if (actual instanceof WorkflowCancelledException || actual instanceof InterruptedException) {
        // don't mark the workflow status as error yet. this is cancel
        // if this is a parent cancel, the exception is thrown to caller
        // state is already c
        // if this is child cancel, its state is already Cancelled
        // in parent it will fall thru to PostInvoke call below to set state to
        // Error
        throw new AwaitedWorkflowCancelledException(workflowId);
      }

      postInvokeWorkflowError(systemDatabase, workflowId, actual);
      throw actual instanceof Exception ? (Exception) actual : e;
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T syncWorkflow(
      String workflowName,
      String targetClassName,
      Object target,
      Object[] args,
      WorkflowFunctionReflect function,
      String workflowId)
      throws Exception {

    String wfid = workflowId;

    WorkflowInitResult initResult = null;

    DBOSContext ctx = DBOSContextHolder.get();
    ctx.setDbos(dbos);
    if (ctx.hasParent()) {
      Optional<String> childId =
          systemDatabase.checkChildWorkflow(ctx.getParentWorkflowId(), ctx.getParentFunctionId());
      if (childId.isPresent()) {
        return (T) systemDatabase.awaitWorkflowResult(childId.get());
      }
    }

    var parent = ParentWorkflow.fromContext();
    long workflowTimeoutMs = DBOSContextHolder.get().getWorkflowTimeoutMs();
    initResult =
        preInvokeWorkflow(
            systemDatabase,
            workflowName,
            targetClassName,
            args,
            wfid,
            null,
            getExecutorId(),
            getAppVersion(),
            parent,
            workflowTimeoutMs);

    if (initResult.getStatus().equals(WorkflowState.SUCCESS.name())) {
      return (T) systemDatabase.getWorkflowResult(initResult.getWorkflowId()).get();
    } else if (initResult.getStatus().equals(WorkflowState.ERROR.name())) {
      logger.warn("Idempotency check not impl for error");
    } else if (initResult.getStatus().equals(WorkflowState.CANCELLED.name())) {
      logger.warn("Idempotency check not impl for cancelled");
    }

    long allowedTime = initResult.getDeadlineEpochMS() - System.currentTimeMillis();
    if (initResult.getDeadlineEpochMS() > 0 && allowedTime < 0) {
      systemDatabase.cancelWorkflow(workflowId);
      return null;
    }

    if (allowedTime > 0) {
      @SuppressWarnings("unused")
      ScheduledFuture<?> timeoutTask =
          timeoutScheduler.schedule(
              () -> {
                WorkflowStatus status = systemDatabase.getWorkflowStatus(wfid);
                if (status.getStatus() != WorkflowState.SUCCESS.name()
                    && status.getStatus() != WorkflowState.ERROR.name()) {
                  systemDatabase.cancelWorkflow(wfid);
                }
              },
              allowedTime,
              TimeUnit.MILLISECONDS);
    }

    return runWorkflowAndSaveResult(target, args, function, workflowId);
  }

  public <T> WorkflowHandle<T, ?> submitWorkflow(
      String workflowName,
      String targetClassName,
      Object target,
      Object[] args,
      WorkflowFunctionReflect function)
      throws Exception {

    DBOSContext ctx = DBOSContextHolder.get();
    ctx.setDbos(dbos);

    String workflowId = ctx.getWorkflowId();

    final String wfId = workflowId;

    if (ctx.hasParent()) {
      Optional<String> childId =
          systemDatabase.checkChildWorkflow(ctx.getParentWorkflowId(), ctx.getParentFunctionId());
      if (childId.isPresent()) {
        logger.info("child Id is present {}", childId);
        return new WorkflowHandleDBPoll<>(childId.get(), systemDatabase);
      }
    }

    var parent = ParentWorkflow.fromContext();
    long workflowTimeoutMs = DBOSContextHolder.get().getWorkflowTimeoutMs();
    WorkflowInitResult initResult =
        preInvokeWorkflow(
            systemDatabase,
            workflowName,
            targetClassName,
            args,
            wfId,
            null,
            getExecutorId(),
            getAppVersion(),
            parent,
            workflowTimeoutMs);

    if (initResult.getStatus().equals(WorkflowState.SUCCESS.name())) {
      return new WorkflowHandleDBPoll<>(wfId, systemDatabase);
    } else if (initResult.getStatus().equals(WorkflowState.ERROR.name())) {
      logger.warn("Idempotency check not impl for error");
    } else if (initResult.getStatus().equals(WorkflowState.CANCELLED.name())) {
      logger.warn("Idempotency check not impl for cancelled");
    }

    // Copy the context - dont just pass a reference - memory visibility
    var contextForInsideCall = DBOSContextHolder.get().copy();
    Callable<T> task =
        () -> {
          T result = null;

          // Doing this on purpose to ensure that we have the correct context
          DBOSContextHolder.set(contextForInsideCall);
          var context = DBOSContextHolder.get();
          context.setDbos(dbos);
          String id = context.getWorkflowId();

          try {

            result = runWorkflowAndSaveResult(target, args, function, id);

          } catch (Exception e) {
            Throwable actual =
                (e instanceof InvocationTargetException)
                    ? ((InvocationTargetException) e).getTargetException()
                    : e;

            logger.error("Error executing workflow", actual);
          } finally {
            DBOSContextHolder.clear();
          }

          return result;
        };

    long allowedTime = initResult.getDeadlineEpochMS() - System.currentTimeMillis();

    if (initResult.getDeadlineEpochMS() > 0 && allowedTime < 0) {
      logger.info("Timeout deadline exceeded. Cancelling workflow {}", workflowId);
      systemDatabase.cancelWorkflow(workflowId);
      return new WorkflowHandleDBPoll<>(wfId, systemDatabase);
    }

    Future<T> future = executorService.submit(task);

    if (allowedTime > 0) {
      @SuppressWarnings("unused")
      ScheduledFuture<?> timeoutTask =
          timeoutScheduler.schedule(
              () -> {
                if (!future.isDone()) {
                  logger.info(" Workflow timed out {}", wfId);
                  future.cancel(false);
                  systemDatabase.cancelWorkflow(wfId);
                }
              },
              allowedTime,
              TimeUnit.MILLISECONDS);
    }

    return new WorkflowHandleFuture<T, Exception>(workflowId, future, systemDatabase);
  }

  // TODO: add priority + deduplicationId support
  // (https://github.com/dbos-inc/dbos-transact-java/issues/67)
  public static String enqueueWorkflow(
      SystemDatabase systemDatabase,
      String wfid,
      String workflowName,
      String targetClassName,
      Object[] args,
      String queueName,
      String executorId,
      String appVersion,
      ParentWorkflow parent,
      long workflowTimeoutMs)
      throws Exception {

    if (wfid == null) {
      wfid = UUID.randomUUID().toString();
    }

    WorkflowInitResult initResult = null;
    try {
      initResult =
          preInvokeWorkflow(
              systemDatabase,
              workflowName,
              targetClassName,
              args,
              wfid,
              queueName,
              executorId,
              appVersion,
              parent,
              workflowTimeoutMs);
    } catch (Exception e) {
      Throwable actual =
          (e instanceof InvocationTargetException)
              ? ((InvocationTargetException) e).getTargetException()
              : e;
      logger.error("Error enqueing workflow", actual);
      postInvokeWorkflowError(systemDatabase, initResult.getWorkflowId(), actual);
      throw actual instanceof Exception ? (Exception) actual : e;
    }

    return wfid;
  }

  public void enqueueWorkflow(
      String workflowName, String targetClassName, Object[] args, Queue queue) throws Exception {

    DBOSContext ctx = DBOSContextHolder.get();
    String wfid = ctx.getWorkflowId();
    var parent = ParentWorkflow.fromContext(ctx);
    long workflowTimeoutMs = ctx.getWorkflowTimeoutMs();

    enqueueWorkflow(
        systemDatabase,
        wfid,
        workflowName,
        targetClassName,
        args,
        queue.getName(),
        getExecutorId(),
        getAppVersion(),
        parent,
        workflowTimeoutMs);
  }

  /** This does not retry */
  private <T> T callFunctionAsStep(Supplier<T> fn, String functionName) {
    DBOSContext ctx = DBOSContextHolder.get();

    int nextFuncId = 0;
    boolean inWorkflow = ctx != null && ctx.isInWorkflow();

    if (!inWorkflow) return fn.get();

    nextFuncId = ctx.getAndIncrementFunctionId();

    StepResult result =
        systemDatabase.checkStepExecutionTxn(ctx.getWorkflowId(), nextFuncId, functionName);
    if (result != null) {
      return handleExistingResult(result, functionName);
    }

    T functionResult;

    try {
      functionResult = fn.get();
    } catch (Exception e) {
      if (inWorkflow) {
        String jsonError = JSONUtil.serializeAppException(e);
        StepResult r =
            new StepResult(ctx.getWorkflowId(), nextFuncId, functionName, null, jsonError);
        systemDatabase.recordStepResultTxn(r);
      }

      if (e instanceof NonExistentWorkflowException) {
        throw e;
      } else {
        throw new DBOSException(
            UNEXPECTED.getCode(), "Function execution failed: " + functionName, e);
      }
    }

    // Record the successful result
    String jsonOutput = JSONUtil.serialize(functionResult);
    StepResult o = new StepResult(ctx.getWorkflowId(), nextFuncId, functionName, jsonOutput, null);
    systemDatabase.recordStepResultTxn(o);

    return functionResult;
  }

  // TODO: should these also throw DBOS exceptions?
  // Should there be an unchecked version that promotes errors to unchecked?
  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T runStepI(ThrowingSupplier<T, E> stepfunc, StepOptions opts)
      throws E {
    try {
      return runStepInternal(
          opts.name(),
          opts.retriesAllowed(),
          opts.maxAttempts(),
          opts.backOffRate(),
          opts.intervalSeconds(),
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
  public <T> T runStepInternal(
      String stepName,
      boolean retryAllowed,
      int maxAttempts,
      double timeBetweenAttemptsSec,
      double backOffRate,
      ThrowingSupplier<T, Exception> function)
      throws Exception {
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

    ctx.setDbos(dbos);
    String workflowId = ctx.getWorkflowId();

    logger.info("Running step {} for workflow {}", stepName, workflowId);

    int stepFunctionId = ctx.getAndIncrementFunctionId();

    StepResult recordedResult =
        systemDatabase.checkStepExecutionTxn(workflowId, stepFunctionId, stepName);

    if (recordedResult != null) {
      String output = recordedResult.getOutput();
      if (output != null) {
        logger.info("Result has an output");
        Object[] stepO = JSONUtil.deserializeToArray(output);
        return stepO == null ? null : (T) stepO[0];
      }

      String error = recordedResult.getError();
      if (error != null) {
        var throwable = JSONUtil.deserializeAppException(error);
        if (!(throwable instanceof Exception))
          throw new RuntimeException(throwable.getMessage(), throwable);
        throw (Exception) throwable;
      }
    }

    int currAttempts = 1;
    String serializedOutput = null;
    Exception eThrown = null;
    T result = null;
    boolean shouldRetry = true;

    while (currAttempts <= maxAttempts) {
      try {
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
      }

      if (!shouldRetry || !retryAllowed) {
        break;
      }

      try {
        Thread.sleep((long) (timeBetweenAttemptsSec * 1000));
      } catch (InterruptedException e) {
      }
      timeBetweenAttemptsSec *= backOffRate;
      ++currAttempts;
    }

    if (eThrown == null) {
      StepResult stepResult =
          new StepResult(workflowId, stepFunctionId, stepName, serializedOutput, null);
      systemDatabase.recordStepResultTxn(stepResult);
      return result;
    } else {
      logger.info("After: step threw exception; saving error");
      StepResult stepResult =
          new StepResult(
              workflowId, stepFunctionId, stepName, null, JSONUtil.serializeAppException(eThrown));
      systemDatabase.recordStepResultTxn(stepResult);
      throw eThrown;
    }
  }

  /** Retrieve the workflowHandle for the workflowId */
  public <T, E extends Exception> WorkflowHandle<T, E> retrieveWorkflow(String workflowId) {
    return new WorkflowHandleDBPoll<T, E>(workflowId, systemDatabase);
  }

  @SuppressWarnings("unchecked")
  public <T, E extends Exception> WorkflowHandle<T, E> executeWorkflowById(String workflowId) {

    WorkflowStatus status = systemDatabase.getWorkflowStatus(workflowId);

    if (status == null) {
      logger.error("Workflow not found {}", workflowId);
      throw new NonExistentWorkflowException(workflowId);
    }

    Object[] inputs = status.getInput();
    RegisteredWorkflow functionWrapper = workflowMap.get(status.getName());

    if (functionWrapper == null) {
      throw new WorkflowFunctionNotFoundException(workflowId);
    }

    WorkflowHandle<T, E> handle = null;
    try (SetWorkflowID id = new SetWorkflowID(workflowId)) {
      var ctx = DBOSContextHolder.get();
      ctx.setInWorkflow(true);
      ctx.setDbos(dbos);

      try {
        handle =
            (WorkflowHandle<T, E>)
                submitWorkflow(
                    status.getName(),
                    functionWrapper.className(),
                    functionWrapper.target(),
                    inputs,
                    functionWrapper.function());
      } catch (Exception t) {
        logger.error("Error executing workflow by id : {}", workflowId, t);
      }
    }

    return handle;
  }

  public void sleep(float seconds) {
    // CB TODO: This should be OK outside DBOS

    DBOSContext context = DBOSContextHolder.get();
    context.setDbos(dbos);

    if (context.getWorkflowId() == null) {
      throw new DBOSException(
          ErrorCode.SLEEP_NOT_IN_WORKFLOW.getCode(),
          "sleep() must be called from within a workflow");
    }

    systemDatabase.sleep(
        context.getWorkflowId(), context.getAndIncrementFunctionId(), seconds, false);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> resumeWorkflow(String workflowId) {

    Supplier<Void> resumeFunction =
        () -> {
          logger.info("Resuming workflow {}", workflowId);
          systemDatabase.resumeWorkflow(workflowId);
          return null; // void
        };
    // Execute the resume operation as a workflow step
    this.callFunctionAsStep(resumeFunction, "DBOS.resumeWorkflow");
    return retrieveWorkflow(workflowId);
  }

  public void cancelWorkflow(String workflowId) {

    Supplier<Void> cancelFunction =
        () -> {
          logger.info("Cancelling workflow {}", workflowId);
          systemDatabase.cancelWorkflow(workflowId);
          return null; // void
        };
    // Execute the cancel operation as a workflow step
    this.callFunctionAsStep(cancelFunction, "DBOS.resumeWorkflow");
  }

  public <T, E extends Exception> WorkflowHandle<T, E> forkWorkflow(
      String workflowId, int startStep, ForkOptions options) {

    Supplier<String> forkFunction =
        () -> {
          logger.info("Forking workflow:{} from step:{} ", workflowId, startStep);

          return systemDatabase.forkWorkflow(workflowId, startStep, options);
        };

    String forkedId = this.callFunctionAsStep(forkFunction, "DBOS.forkedWorkflow");
    return retrieveWorkflow(forkedId);
  }

  public <T, E extends Exception> WorkflowHandle<T, E> startWorkflow(ThrowingSupplier<T, E> func) {
    DBOSContext oldctx = DBOSContextHolder.get();
    oldctx.setDbos(dbos);
    DBOSContext newCtx = oldctx;

    if (newCtx.getWorkflowId() == null) {
      newCtx = newCtx.copyWithWorkflowId(UUID.randomUUID().toString());
    }

    if (newCtx.getQueue() == null) {
      newCtx = newCtx.copyWithAsync();
    }

    try {
      DBOSContextHolder.set(newCtx);
      func.execute();
      return retrieveWorkflow(newCtx.getWorkflowId());
    } catch (Exception t) {
      throw new DBOSException(UNEXPECTED.getCode(), t.getMessage());
    } finally {
      DBOSContextHolder.set(oldctx);
    }
  }

  public void globalTimeout(Long cutoff) {
    OffsetDateTime endTime = Instant.ofEpochMilli(cutoff).atOffset(ZoneOffset.UTC);
    globalTimeout(endTime);
  }

  public void globalTimeout(OffsetDateTime endTime) {
    try {
      ListWorkflowsInput pendingInput =
          new ListWorkflowsInput.Builder().status(WorkflowState.PENDING).endTime(endTime).build();
      for (WorkflowStatus status : systemDatabase.listWorkflows(pendingInput)) {
        cancelWorkflow(status.getWorkflowId());
      }

      ListWorkflowsInput enqueuedInput =
          new ListWorkflowsInput.Builder().status(WorkflowState.ENQUEUED).endTime(endTime).build();
      for (WorkflowStatus status : systemDatabase.listWorkflows(enqueuedInput)) {
        cancelWorkflow(status.getWorkflowId());
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void send(
      String destinationId,
      Object message,
      String topic,
      InternalWorkflowsService internalWorkflowsService) {

    DBOSContext ctx = DBOSContextHolder.get();
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
   * @param timeoutSeconds time in seconds after which the call times out
   * @return the message if there is one or else null
   */
  public Object recv(String topic, float timeoutSeconds) {
    DBOSContext ctx = DBOSContextHolder.get();
    if (!ctx.isInWorkflow()) {
      throw new IllegalArgumentException("recv() must be called from a workflow.");
    }
    int stepFunctionId = ctx.getAndIncrementFunctionId();
    int timeoutFunctionId = ctx.getAndIncrementFunctionId();

    return systemDatabase.recv(
        ctx.getWorkflowId(), stepFunctionId, timeoutFunctionId, topic, timeoutSeconds);
  }

  public void setEvent(String key, Object value) {
    logger.info("Received setEvent for key {}", key);

    DBOSContext ctx = DBOSContextHolder.get();
    if (!ctx.isInWorkflow()) {
      throw new IllegalArgumentException("send must be called from a workflow.");
    }
    int stepFunctionId = ctx.getAndIncrementFunctionId();

    systemDatabase.setEvent(ctx.getWorkflowId(), stepFunctionId, key, value);
  }

  public Object getEvent(String workflowId, String key, float timeOut) {
    logger.info("Received getEvent for {} {}", workflowId, key);

    DBOSContext ctx = DBOSContextHolder.get();

    if (ctx.isInWorkflow()) {
      int stepFunctionId = ctx.getAndIncrementFunctionId();
      int timeoutFunctionId = ctx.getAndIncrementFunctionId();
      GetWorkflowEventContext callerCtx =
          new GetWorkflowEventContext(ctx.getWorkflowId(), stepFunctionId, timeoutFunctionId);
      return systemDatabase.getEvent(workflowId, key, timeOut, callerCtx);
    }

    return systemDatabase.getEvent(workflowId, key, timeOut, null);
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    Supplier<List<WorkflowStatus>> listWorkflowFunction =
        () -> {
          logger.info("List workflows");

          try {
            return systemDatabase.listWorkflows(input);
          } catch (SQLException sq) {
            logger.error("Unexpected SQL exception", sq);
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
          }
        };

    return this.callFunctionAsStep(listWorkflowFunction, "DBOS.listWorkflows");
  }

  public List<StepInfo> listWorkflowSteps(String workflowId) {
    Supplier<List<StepInfo>> listWorkflowStepsFunction =
        () -> {
          logger.info("List workflow steps");

          try {
            return systemDatabase.listWorkflowSteps(workflowId);
          } catch (SQLException sq) {
            logger.error("Unexpected SQL exception", sq);
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
          }
        };

    return this.callFunctionAsStep(listWorkflowStepsFunction, "DBOS.listWorkflowSteps");
  }

  public List<WorkflowStatus> listQueuedWorkflows(
      ListQueuedWorkflowsInput query, boolean loadInput) {
    Supplier<List<WorkflowStatus>> listQueuedWorkflowsFunction =
        () -> {
          logger.info("List queued workflows");

          try {
            return systemDatabase.listQueuedWorkflows(query, loadInput);
          } catch (SQLException sq) {
            logger.error("Unexpected SQL exception", sq);
            throw new DBOSException(UNEXPECTED.getCode(), sq.getMessage());
          }
        };

    return this.callFunctionAsStep(listQueuedWorkflowsFunction, "DBOS.listQueuedWorkflows");
  }
}
