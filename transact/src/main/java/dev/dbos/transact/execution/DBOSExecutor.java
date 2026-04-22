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
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.database.GetWorkflowEventContext;
import dev.dbos.transact.database.Result;
import dev.dbos.transact.database.StreamIterator;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.database.WorkflowInitResult;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.exceptions.DBOSWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException;
import dev.dbos.transact.internal.AppVersionComputer;
import dev.dbos.transact.internal.WorkflowRegistry;
import dev.dbos.transact.json.ArgumentCoercion;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.StepOptions;
import dev.dbos.transact.workflow.Timeout;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowDelay;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;
import dev.dbos.transact.workflow.internal.WorkflowHandleFuture;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.cronutils.model.time.ExecutionTime;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSExecutor implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(DBOSExecutor.class);

  // Invocation and hookHolder are used by startWorkflow to capture
  // workflow information w/o executing workflow function

  record Invocation(
      DBOSExecutor executor,
      String workflowName,
      String className,
      String instanceName,
      Object[] args) {

    public String fqName() {
      return RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName);
    }
  }

  private static final ThreadLocal<Consumer<Invocation>> hookHolder = new ThreadLocal<>();

  private final DBOSConfig config;
  private final DBOSSerializer serializer;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  private boolean dbosCloud;
  private String appVersion;
  private String executorId;
  private String appId;

  private Set<DBOSLifecycleListener> listeners;
  private Map<String, RegisteredWorkflow> workflowMap;
  private Map<String, RegisteredWorkflowInstance> instanceMap;
  private Map<String, Queue> queueMap;
  private ConcurrentHashMap<String, Boolean> workflowsInProgress = new ConcurrentHashMap<>();

  private SystemDatabase systemDatabase;
  private QueueService queueService;
  private SchedulerService schedulerService;
  private AdminServer adminServer;
  private Conductor conductor;
  private ExecutorService executorService;
  private ScheduledExecutorService timeoutScheduler;
  private AlertHandler alertHandler;

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
      DBOS dbos,
      Set<DBOSLifecycleListener> listenerSet,
      Map<String, RegisteredWorkflow> workflowMap,
      Map<String, RegisteredWorkflowInstance> instanceMap,
      List<Queue> queues,
      AlertHandler alertHandler) {

    if (isRunning.compareAndSet(false, true)) {
      logger.info("DBOS Executor starting");

      this.workflowMap = Collections.unmodifiableMap(workflowMap);
      this.instanceMap = Collections.unmodifiableMap(instanceMap);
      this.queueMap =
          queues.stream().collect(Collectors.toUnmodifiableMap(Queue::name, queue -> queue));
      this.listeners = listenerSet;
      this.alertHandler = alertHandler;

      if (this.appVersion == null || this.appVersion.isEmpty()) {
        this.appVersion =
            AppVersionComputer.computeAppVersion(DBOS.version(), workflowMap.values());
      }

      if (config.conductorKey() != null) {
        this.executorId = UUID.randomUUID().toString();
      }

      logger.info("System Database: {}", this.config.databaseUrl());
      logger.info("System Database User name: {}", this.config.dbUser());
      logger.info("Executor ID: {}", this.executorId);
      logger.info("Application Version: {}", this.appVersion);

      Supplier<ExecutorService> executorServiceSupplier =
          () -> {
            try {
              // use virtual thread executor when available (i.e. Java 21+)
              var method = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
              var svc = (ExecutorService) method.invoke(null);
              logger.debug("using newVirtualThreadPerTaskExecutor");
              return svc;

            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
              // fall back to fixed thread pool executor if virtual thread executor unavailable
              logger.debug("using newFixedThreadPool");
              int threadCount = Runtime.getRuntime().availableProcessors() * 50;
              return Executors.newFixedThreadPool(threadCount);
            }
          };

      executorService = executorServiceSupplier.get();
      timeoutScheduler = Executors.newScheduledThreadPool(2);

      systemDatabase = SystemDatabase.create(config);
      systemDatabase.start();

      systemDatabase.createApplicationVersion(this.appVersion);
      var latest = systemDatabase.getLatestApplicationVersion();
      if (!latest.versionName().equals(this.appVersion)) {
        logger.warn(
            "Current version {} is not the latest version. Latest version is {}",
            this.appVersion,
            latest.versionName());
      }

      queueService = new QueueService(this, systemDatabase);
      queueService.start(queueMap.values(), config.listenQueues());

      var schedulerPollingInterval =
          Objects.requireNonNullElseGet(
              config.schedulerPollingInterval(), () -> Duration.ofSeconds(30));
      schedulerService = new SchedulerService(this, systemDatabase, schedulerPollingInterval);
      schedulerService.start();

      for (var listener : listeners) {
        listener.dbosLaunched(dbos);
      }

      Runnable recoveryTask =
          () -> {
            try {
              recoverPendingWorkflows(List.of(executorId()));
            } catch (Throwable t) {
              logger.error("Recovery task failed", t);
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

      if (schedulerService != null) {
        schedulerService.close();
      }
      schedulerService = null;
      if (queueService != null) {
        queueService.close();
      }
      queueService = null;
      if (systemDatabase != null) {
        systemDatabase.close();
      }
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

  // executor metadata

  public String executorId() {
    return this.executorId;
  }

  public String appVersion() {
    return this.appVersion;
  }

  public String appId() {
    return this.appId;
  }

  public String appName() {
    return config.appName();
  }

  public Map<String, Object> executorMetadata() {
    return this.config.conductorExecutorMetadata();
  }

  public Collection<RegisteredWorkflow> getRegisteredWorkflows() {
    return this.workflowMap.values();
  }

  public Collection<RegisteredWorkflowInstance> getRegisteredWorkflowInstances() {
    return this.instanceMap.values();
  }

  public Optional<RegisteredWorkflow> getRegisteredWorkflow(
      String workflowName, String className, String instanceName) {
    var fqName = RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName);
    return Optional.ofNullable(this.workflowMap.get(fqName));
  }

  public Collection<Queue> getQueues() {
    return this.queueMap.values();
  }

  public Optional<Queue> getQueue(String queueName) {
    return Optional.ofNullable(this.queueMap.get(queueName));
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

  // DBOS / DBOSClient API methods

  public void send(
      String destinationId,
      Object message,
      String topic,
      String idempotencyKey,
      SerializationStrategy serialization) {

    DBOSContext ctx = DBOSContextHolder.get();
    if (ctx.isInWorkflow() && !ctx.isInStep()) {
      int stepId = ctx.getAndIncrementFunctionId();
      systemDatabase.send(
          ctx.getWorkflowId(),
          stepId,
          destinationId,
          message,
          topic,
          idempotencyKey,
          serialization.formatName());
    } else {
      systemDatabase.sendDirect(
          destinationId, message, topic, idempotencyKey, serialization.formatName());
    }
  }

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

  public Map<String, Object> getAllEvents(String workflowId) {
    return this.runDbosFunctionAsStep(
        () -> systemDatabase.getAllEvents(workflowId), "DBOS.getAllEvents", null);
  }

  public void sleep(Duration duration) {
    DBOSContext context = DBOSContextHolder.get();

    if (context.getWorkflowId() == null) {
      throw new IllegalStateException("sleep() must be called from within a workflow");
    }

    systemDatabase.sleep(context.getWorkflowId(), context.getAndIncrementFunctionId(), duration);
  }

  public void cancelWorkflows(List<String> workflowIds) {
    Objects.requireNonNull(workflowIds);

    // Execute the cancel operation as a workflow step
    this.runDbosFunctionAsStep(
        () -> {
          logger.info("Cancelling workflow(s) {}", workflowIds);
          systemDatabase.cancelWorkflows(workflowIds);
          return null; // void
        },
        "DBOS.cancelWorkflow",
        null);
  }

  public void deleteWorkflows(List<String> workflowIds, boolean deleteChildren) {
    Objects.requireNonNull(workflowIds);
    this.runDbosFunctionAsStep(
        () -> {
          logger.info(
              "Deleting workflow(s) {}{}",
              workflowIds,
              deleteChildren ? " and their children" : "");
          systemDatabase.deleteWorkflows(workflowIds, deleteChildren);
          return null;
        },
        "DBOS.deleteWorkflow",
        null);
  }

  public List<WorkflowHandle<Object, Exception>> resumeWorkflows(
      List<String> workflowIds, String queueName) {
    Objects.requireNonNull(workflowIds);

    // Execute the resume operation as a workflow step
    this.runDbosFunctionAsStep(
        () -> {
          logger.info("Resuming workflow(s) {}", workflowIds);
          systemDatabase.resumeWorkflows(workflowIds, queueName);
          return null; // void
        },
        "DBOS.resumeWorkflow",
        null);

    return workflowIds.stream().map(this::retrieveWorkflow).toList();
  }

  public <T, E extends Exception> WorkflowHandle<T, E> forkWorkflow(
      String workflowId, int startStep, ForkOptions options) {

    String forkedId =
        this.runDbosFunctionAsStep(
            () -> {
              logger.info("Forking workflow:{} from step:{} ", workflowId, startStep);

              validateQueue(options.queueName(), options.queuePartitionKey());

              return systemDatabase.forkWorkflow(workflowId, startStep, options);
            },
            "DBOS.forkWorkflow",
            null);
    return retrieveWorkflow(forkedId);
  }

  public <R, E extends Exception> WorkflowHandle<R, E> retrieveWorkflow(String workflowId) {
    logger.debug("retrieveWorkflow {}", workflowId);
    return new WorkflowHandleDBPoll<>(this, workflowId);
  }

  public void setWorkflowDelay(@NonNull String workflowId, @NonNull WorkflowDelay delay) {
    Objects.requireNonNull(workflowId);
    this.runDbosFunctionAsStep(
        () -> {
          logger.info("setWorkflowDelay workflow {} delay {}", workflowId, delay);
          systemDatabase.setWorkflowDelay(workflowId, delay);
          return null; // void
        },
        "DBOS.setWorkflowDelay",
        null);
  }

  public void writeStream(String key, Object value, SerializationStrategy serialization) {
    logger.debug("Received writeStream for key {}", key);

    DBOSContext ctx = DBOSContextHolder.get();
    if (!ctx.isInWorkflow()) {
      throw new IllegalStateException("DBOS.writeStream() must be called from a workflow.");
    }

    if (serialization == null || serialization.equals(SerializationStrategy.DEFAULT)) {
      if (ctx.getSerialization() != null) {
        serialization = ctx.getSerialization();
      } else {
        serialization = SerializationStrategy.DEFAULT;
      }
    }

    if (ctx.isInStep()) {
      systemDatabase.writeStreamFromStep(
          ctx.getWorkflowId(), ctx.getCurrentFunctionId(), key, value, serialization.formatName());
    } else {
      int functionId = ctx.getAndIncrementFunctionId();
      systemDatabase.writeStreamFromWorkflow(
          ctx.getWorkflowId(), functionId, key, value, serialization.formatName());
    }
  }

  public void closeStream(String key) {
    logger.debug("Received closeStream for key {}", key);

    DBOSContext ctx = DBOSContextHolder.get();
    if (!ctx.isInWorkflow()) {
      throw new IllegalStateException("DBOS.closeStream() must be called from a workflow.");
    }

    if (ctx.isInStep()) {
      throw new IllegalStateException(
          "DBOS.closeStream() must be called from a workflow, not a step.");
    }

    int functionId = ctx.getAndIncrementFunctionId();
    systemDatabase.closeStream(ctx.getWorkflowId(), functionId, key);
  }

  public Iterator<Object> readStream(String workflowId, String key) {
    logger.debug("Received readStream for {} {}", workflowId, key);
    return new StreamIterator(workflowId, key, systemDatabase);
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return this.runDbosFunctionAsStep(
        () -> systemDatabase.listWorkflows(input), "DBOS.listWorkflows", null);
  }

  public List<StepInfo> listWorkflowSteps(
      String workflowId, Boolean loadOutput, Integer limit, Integer offset) {
    return this.runDbosFunctionAsStep(
        () -> systemDatabase.listWorkflowSteps(workflowId, loadOutput, limit, offset),
        "DBOS.listWorkflowSteps",
        null);
  }

  public List<VersionInfo> listApplicationVersions() {
    return systemDatabase.listApplicationVersions();
  }

  public VersionInfo getLatestApplicationVersion() {
    return systemDatabase.getLatestApplicationVersion();
  }

  public void setLatestApplicationVersion(String versionName) {
    systemDatabase.updateApplicationVersionTimestamp(versionName, Instant.now());
  }

  public void createSchedule(@NonNull WorkflowSchedule schedule) {

    Objects.requireNonNull(schedule, "schedule cannot be null");
    validateQueue(schedule.queueName());
    validateWorkflow(schedule.workflowName(), schedule.className());

    this.runDbosFunctionAsStep(
        () -> {
          systemDatabase.createSchedule(schedule);
          return null;
        },
        "DBOS.createSchedule",
        null);
  }

  public Optional<WorkflowSchedule> getSchedule(String name) {
    return this.runDbosFunctionAsStep(
        () -> systemDatabase.getSchedule(name), "DBOS.getSchedule", null);
  }

  public List<WorkflowSchedule> listSchedules(
      List<ScheduleStatus> statuses, List<String> workflowNames, List<String> namePrefixes) {
    return this.runDbosFunctionAsStep(
        () -> systemDatabase.listSchedules(statuses, workflowNames, namePrefixes),
        "DBOS.listSchedules",
        null);
  }

  public void deleteSchedule(String name) {
    this.runDbosFunctionAsStep(
        () -> {
          systemDatabase.deleteSchedule(name);
          return null;
        },
        "DBOS.deleteSchedule",
        null);
  }

  public void pauseSchedule(String name) {
    this.runDbosFunctionAsStep(
        () -> {
          systemDatabase.pauseSchedule(name);
          return null;
        },
        "DBOS.pauseSchedule",
        null);
  }

  public void resumeSchedule(String name) {
    this.runDbosFunctionAsStep(
        () -> {
          systemDatabase.resumeSchedule(name);
          return null;
        },
        "DBOS.resumeSchedule",
        null);
  }

  public void applySchedules(List<WorkflowSchedule> schedules) {

    if (DBOSContextHolder.get().isInWorkflow()) {
      throw new IllegalStateException(
          "DBOS.applySchedules cannot be called from within a workflow");
    }

    for (WorkflowSchedule s : schedules) {
      validateQueue(s.queueName());
      validateWorkflow(s.workflowName(), s.className());
    }

    systemDatabase.applySchedules(
        schedules.stream()
            .map(
                s -> {
                  return s.withScheduleId(UUID.randomUUID().toString())
                      .withStatus(ScheduleStatus.ACTIVE)
                      .withLastFiredAt(null);
                })
            .toList());
  }

  public List<WorkflowHandle<Object, Exception>> backfillSchedule(
      @NonNull String scheduleName, @NonNull Instant start, @NonNull Instant end) {

    if (DBOSContextHolder.get().isInWorkflow()) {
      throw new IllegalStateException(
          "DBOS.backfillSchedule cannot be called from within a workflow");
    }

    var workflowIds =
        DBOSExecutor.backfillSchedule(scheduleName, start, end, systemDatabase, serializer);
    return workflowIds.stream().map(this::retrieveWorkflow).toList();
  }

  public static List<String> backfillSchedule(
      @NonNull String scheduleName,
      @NonNull Instant start,
      @NonNull Instant end,
      @NonNull SystemDatabase systemDatabase,
      @Nullable DBOSSerializer serializer) {

    var schedule =
        Objects.requireNonNull(systemDatabase, "systemDatabase cannot be null")
            .getSchedule(Objects.requireNonNull(scheduleName, "scheduleName cannot be null"))
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Schedule %s does not exist".formatted(scheduleName)));

    var timeZone = Objects.requireNonNullElse(schedule.cronTimezone(), ZoneOffset.UTC);
    var cron = SchedulerService.CRON_PARSER.parse(schedule.cron());
    var executionTime = ExecutionTime.forCron(cron);

    var next = Objects.requireNonNull(start, "start cannot be null").atZone(timeZone);
    var zonedEnd = Objects.requireNonNull(end, "end cannot be null").atZone(timeZone);
    var workflowIds = new ArrayList<String>();

    while (true) {
      next = executionTime.nextExecution(next).orElse(null);
      if (next == null || next.isAfter(zonedEnd)) {
        break;
      }

      var workflowId = "sched-%s-%s".formatted(schedule.scheduleName(), next);
      enqueueScheduledWorkflow(
          schedule.workflowName(),
          schedule.className(),
          workflowId,
          schedule.context(),
          schedule.queueName(),
          next.toInstant(),
          systemDatabase,
          serializer);

      workflowIds.add(workflowId);
    }
    return workflowIds;
  }

  public <T, E extends Exception> @NonNull WorkflowHandle<T, E> triggerSchedule(
      @NonNull String scheduleName) {
    if (DBOSContextHolder.get().isInWorkflow()) {
      throw new IllegalStateException(
          "DBOS.triggerSchedule cannot be called from within a workflow");
    }

    var workflowId = triggerSchedule(scheduleName, systemDatabase, serializer);
    return retrieveWorkflow(workflowId);
  }

  public static String triggerSchedule(
      @NonNull String scheduleName, SystemDatabase systemDatabase, DBOSSerializer serializer) {
    var schedule =
        Objects.requireNonNull(systemDatabase)
            .getSchedule(Objects.requireNonNull(scheduleName, "scheduleName cannot be null"))
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Schedule %s does not exist".formatted(scheduleName)));

    var now = Instant.now();
    var workflowId = "sched-%s-trigger-%s".formatted(schedule.scheduleName(), now);
    enqueueScheduledWorkflow(
        schedule.workflowName(),
        schedule.className(),
        workflowId,
        schedule.context(),
        schedule.queueName(),
        now,
        systemDatabase,
        serializer);
    return workflowId;
  }

  private static void enqueueScheduledWorkflow(
      @NonNull String workflowName,
      @NonNull String className,
      @NonNull String workflowId,
      Object context,
      String queueName,
      @NonNull Instant scheduledAt,
      SystemDatabase systemDatabase,
      DBOSSerializer serializer) {
    var latestAppVersion = systemDatabase.getLatestApplicationVersion().versionName();
    queueName = Objects.requireNonNullElse(queueName, Constants.DBOS_INTERNAL_QUEUE);
    var args = new Object[] {Objects.requireNonNull(scheduledAt), context};

    var options =
        new ExecutionOptions(
            workflowId,
            null,
            null,
            queueName,
            null,
            null,
            null,
            null,
            latestAppVersion,
            false,
            false,
            null);
    enqueueWorkflow(
        workflowName,
        className,
        null,
        null,
        args,
        null,
        options,
        null,
        null,
        null,
        null,
        systemDatabase,
        serializer);
  }

  public Optional<ExternalState> getExternalState(String service, String workflowName, String key) {
    return systemDatabase.getExternalState(service, workflowName, key);
  }

  public ExternalState upsertExternalState(ExternalState state) {
    return systemDatabase.upsertExternalState(state);
  }

  public WorkflowStatus getWorkflowStatus(String workflowId) {
    return this.runDbosFunctionAsStep(
        () -> systemDatabase.getWorkflowStatus(workflowId), "DBOS.getWorkflowStatus", null);
  }

  public <T, E extends Exception> T getResult(String workflowId) throws E {
    return this.runDbosFunctionAsStep(
        () -> awaitWorkflowResult(workflowId), "DBOS.getResult", workflowId);
  }

  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T getResult(String workflowId, Future<T> futureResult) throws E {
    return this.runDbosFunctionAsStep(
        () -> {
          try {
            return futureResult.get();
          } catch (DBOSWorkflowExecutionConflictException e) {
            return awaitWorkflowResult(workflowId);
          } catch (CancellationException e) {
            throw new DBOSAwaitedWorkflowCancelledException(workflowId);
          } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) {
              if (cause instanceof DBOSWorkflowExecutionConflictException) {
                return awaitWorkflowResult(workflowId);
              }
              throw (E) cause;
            }
            throw new RuntimeException("Future threw non-exception", e.getCause());
          } catch (Exception e) {
            throw (E) e;
          }
        },
        "DBOS.getResult",
        workflowId);
  }

  private <T, E extends Exception> T awaitWorkflowResult(String workflowId) throws E {
    var result = systemDatabase.<T>awaitWorkflowResult(workflowId);
    return Result.<T, E>process(result);
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

  // AdminServer / Conductor methods

  public List<WorkflowHandle<?, ?>> recoverPendingWorkflows(List<String> executorIds) {
    Objects.requireNonNull(executorIds);

    var workflows = systemDatabase.getPendingWorkflows(executorIds, appVersion);
    return workflows.stream()
        .map(wf -> recoverWorkflow(wf.workflowId(), wf.queueName()))
        .collect(Collectors.toList());
  }

  WorkflowHandle<?, ?> recoverWorkflow(String workflowId, String queueName) {
    Objects.requireNonNull(workflowId, "workflowId must not be null");

    if (queueName != null) {
      boolean cleared = systemDatabase.clearQueueAssignment(workflowId);
      if (cleared) {
        logger.debug("recoverWorkflow clear queue assignment {}", workflowId);
        return retrieveWorkflow(workflowId);
      }
    }

    logger.debug("recoverWorkflow execute {}", workflowId);
    return executeWorkflowById(workflowId, true, false);
  }

  public void globalTimeout(Instant endTime) {
    var input =
        new ListWorkflowsInput()
            .withEndTime(endTime)
            .withStatus(
                List.of(WorkflowState.PENDING, WorkflowState.ENQUEUED, WorkflowState.DELAYED));
    for (WorkflowStatus status : systemDatabase.listWorkflows(input)) {
      cancelWorkflows(List.of(status.workflowId()));
    }
  }

  // Step Execution methods

  public <T, E extends Exception> T runStep(
      @NonNull ThrowingSupplier<T, E> step,
      @NonNull StepOptions options,
      @Nullable String childWorkflowId)
      throws E {
    var ctx = DBOSContextHolder.get();
    if (!ctx.isInWorkflow()) {
      return Objects.requireNonNull(step, "step must not be null").execute();
    }
    return runStepInternal(step, options, childWorkflowId);
  }

  private <T, E extends Exception> T runDbosFunctionAsStep(
      @NonNull ThrowingSupplier<T, E> step,
      @NonNull String stepName,
      @Nullable String childWorkflowId)
      throws E {
    var ctx = DBOSContextHolder.get();
    if (!ctx.isInWorkflow() || ctx.isInStep()) {
      return Objects.requireNonNull(step, "step must not be null").execute();
    }
    if (Objects.requireNonNull(stepName, "DBOS step name must not be null").isEmpty()) {
      throw new IllegalArgumentException("DBOS step name must not be empty");
    }
    if (!stepName.startsWith("DBOS.")) {
      throw new IllegalArgumentException("DBOS step name must start with DBOS.");
    }
    // DBOS methods are never retried
    return runStepInternal(step, new StepOptions(stepName), childWorkflowId);
  }

  @SuppressWarnings("unchecked")
  private <T, E extends Exception> T runStepInternal(
      @NonNull ThrowingSupplier<T, E> step,
      @NonNull StepOptions options,
      @Nullable String childWorkflowId)
      throws E {

    Objects.requireNonNull(step, "step must not be null");
    Objects.requireNonNull(options, "options must not be null");

    var hook = hookHolder.get();
    if (hook != null) {
      throw new RuntimeException("@Step functions cannot be called from the startWorkflow lambda");
    }

    var ctx = DBOSContextHolder.get();
    var workflowId = ctx.getWorkflowId();
    var stepId = ctx.getAndIncrementFunctionId();
    logger.debug("executeStep #{} ({}) for workflow {}", stepId, options.name(), workflowId);

    var prevResult = systemDatabase.checkStepExecutionTxn(workflowId, stepId, options.name());
    if (prevResult != null) {
      if (prevResult.error() != null) {
        var t =
            SerializationUtil.deserializeError(
                prevResult.error(), prevResult.serialization(), this.serializer);
        if (t instanceof Exception) {
          throw (E) t;
        } else {
          throw new RuntimeException(t.getMessage(), t);
        }
      }

      if (prevResult.output() != null) {
        return (T)
            SerializationUtil.deserializeValue(
                prevResult.output(), prevResult.serialization(), this.serializer);
      }

      throw new IllegalStateException(
          "Recorded output and error are both null for workflow %s step %d (%s)"
              .formatted(workflowId, stepId, options.name()));
    }

    var startTime = System.currentTimeMillis();
    var curAttempts = 1;
    var retryInterval = options.retryInterval();
    T output = null;
    Exception exception = null;

    while (curAttempts <= options.maxAttempts()) {
      boolean callSucceeded = false;

      logger.debug(
          "executeStep #{} for workflow {} attempt {} of {}",
          stepId,
          workflowId,
          curAttempts,
          options.maxAttempts());
      try {
        ctx.setStepFunctionId(stepId);
        output = step.execute();
        exception = null;
        callSucceeded = true;
      } catch (Exception e) {
        var actual = (e instanceof InvocationTargetException ite) ? ite.getTargetException() : e;
        exception = (e instanceof Exception) ? (Exception) actual : e;
      } finally {
        ctx.resetStepFunctionId();
      }

      if (callSucceeded || curAttempts >= options.maxAttempts()) {
        break;
      }

      try {
        Thread.sleep(retryInterval.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      retryInterval = Duration.ofNanos((long) (retryInterval.toNanos() * options.backOffRate()));
      curAttempts++;
    }

    if (exception != null) {
      logger.debug("executeStep #{} for workflow {} threw {}", stepId, workflowId, exception);

      var serializedException = SerializationUtil.serializeError(exception, null, serializer);
      var stepResult =
          new StepResult(
              workflowId,
              stepId,
              options.name(),
              null,
              serializedException.serializedValue(),
              childWorkflowId,
              serializedException.serialization());
      systemDatabase.recordStepResultTxn(stepResult, startTime);
      throw (E) exception;
    } else {
      logger.debug("executeStep #{} for workflow {} completed {}", stepId, workflowId, output);

      var serializedOutput = SerializationUtil.serializeValue(output, null, serializer);
      var stepResult =
          new StepResult(
              workflowId,
              stepId,
              options.name(),
              serializedOutput.serializedValue(),
              null,
              childWorkflowId,
              serializedOutput.serialization());
      systemDatabase.recordStepResultTxn(stepResult, startTime);
      return output;
    }
  }

  // Workflow Execution Methods

  // execute a workflow via a proxy
  public Object runWorkflow(
      Object target, String instanceName, Method method, Object[] args, Workflow wfTag)
      throws Exception {
    var className = WorkflowRegistry.getWorkflowClassName(target);
    var workflowName = WorkflowRegistry.getWorkflowName(wfTag, method);

    // If the hook holder is set, we're invoking the workflow from startWorkflow. In this case,
    // provide the workflow invocation info and return a default value without execution
    var hook = hookHolder.get();
    if (hook != null) {
      hook.accept(new Invocation(this, workflowName, className, instanceName, args));
      var type = method.getReturnType();
      if (type.isPrimitive()) {
        if (type == void.class) return null;
        if (type == boolean.class) return false;
        if (type == byte.class) return (byte) 0;
        if (type == short.class) return (short) 0;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0f;
        if (type == double.class) return 0d;
        if (type == char.class) return '\0';
      }
      return null;
    }

    // if the hook holder isn't set, we're invoking the workflow directly.
    logger.debug(
        "runWorkflow {}({})",
        RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName),
        args);

    var workflow = getRegisteredWorkflow(workflowName, className, instanceName).orElseThrow();

    var ctx = DBOSContextHolder.get();

    WorkflowInfo parent = getParent(ctx);
    String childWorkflowId =
        parent != null ? "%s-%d".formatted(parent.workflowId(), parent.functionId()) : null;

    var workflowId =
        Objects.requireNonNullElseGet(
            ctx.getNextWorkflowId(childWorkflowId), () -> UUID.randomUUID().toString());

    var td = resolveTimeoutAndDeadline(ctx, ctx.getNextTimeout(), ctx.getNextDeadline());

    var options = new ExecutionOptions(workflowId, td.timeout(), td.deadline());
    if (workflow.serializationStrategy() != null) {
      options = options.withSerialization(workflow.serializationStrategy().formatName());
    }

    // submit the workflow for execution and wait on the result.
    var handle = executeWorkflow(workflow, args, options, parent);
    try {
      DBOSContextHolder.clear();
      return handle.getResult();
    } finally {
      DBOSContextHolder.set(ctx);
    }
  }

  // execute a workflow via startWorkflow
  public <T, E extends Exception> WorkflowHandle<T, E> startWorkflow(
      ThrowingSupplier<T, E> wfLambda, StartWorkflowOptions options) {

    logger.debug("startWorkflow {}", options);

    if (options != null
        && options.timeout() instanceof Timeout.Explicit
        && options.deadline() != null) {
      throw new IllegalArgumentException("explicit timeout and deadline cannot both be set");
    }

    // set the invocation hook holder and invoke the lambda to retrieve the invocation information
    Function<ThrowingSupplier<T, E>, Invocation> invocationSupplier =
        (lambda) -> {
          AtomicReference<Invocation> capturedInvocation = new AtomicReference<>();
          DBOSExecutor.hookHolder.set(
              (invocation) -> {
                if (!capturedInvocation.compareAndSet(null, invocation)) {
                  throw new RuntimeException(
                      "Only one @Workflow can be called in the startWorkflow lambda");
                }
              });

          try {
            lambda.execute();
          } catch (Exception e) {
            throw new RuntimeException(e);
          } finally {
            DBOSExecutor.hookHolder.remove();
          }

          return Objects.requireNonNull(
              capturedInvocation.get(), "The startWorkflow lambda must call exactly one @Workflow");
        };

    var invocation = invocationSupplier.apply(wfLambda);
    if (invocation.executor() != this) {
      throw new IllegalStateException(
          "The @Workflow method must be called on the DBOS instance passed to the startWorkflow lambda");
    }
    var workflow =
        getRegisteredWorkflow(
                invocation.workflowName(), invocation.className(), invocation.instanceName())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No registered workflow found for %s".formatted(invocation.fqName())));

    return startRegisteredWorkflow(workflow, invocation.args, options);
  }

  // start a registered workflow, separated out so it can be used by event listeners
  public <T, E extends Exception> WorkflowHandle<T, E> startRegisteredWorkflow(
      RegisteredWorkflow workflow, Object[] args, StartWorkflowOptions options) {
    var ctx = DBOSContextHolder.get();
    var parent = getParent(ctx);
    var childWorkflowId =
        parent != null ? "%s-%d".formatted(parent.workflowId(), parent.functionId()) : null;

    var nextTimeout =
        options != null && options.timeout() != null ? options.timeout() : ctx.getNextTimeout();
    var nextDeadline =
        options != null && options.deadline() != null ? options.deadline() : ctx.getNextDeadline();
    var td = resolveTimeoutAndDeadline(ctx, nextTimeout, nextDeadline);

    var workflowId =
        Objects.requireNonNullElseGet(
            options != null ? options.workflowId() : null,
            () ->
                Objects.requireNonNullElseGet(
                    ctx.getNextWorkflowId(childWorkflowId), () -> UUID.randomUUID().toString()));

    var execOptions =
        new ExecutionOptions(
            workflowId,
            Timeout.of(td.timeout()),
            td.deadline(),
            options != null ? options.queueName() : null,
            options != null ? options.deduplicationId() : null,
            options != null ? options.priority() : null,
            options != null ? options.queuePartitionKey() : null,
            options != null ? options.delay() : null,
            options != null ? options.appVersion() : null,
            false,
            false,
            null);
    return executeWorkflow(workflow, args, execOptions, parent);
  }

  // run an existing workflow via its workflow ID
  public <T, E extends Exception> WorkflowHandle<T, E> executeWorkflowById(
      String workflowId, boolean isRecoveryRequest, boolean isDequeuedRequest) {
    logger.debug("executeWorkflowById {}", workflowId);

    WorkflowStatus status;
    try {
      status = systemDatabase.getWorkflowStatus(workflowId);
    } catch (Exception e) {
      logger.error("Failed to load workflow status for {}", workflowId, e);
      // The serialization column is likely fine — it's the inputs that failed to parse.
      // Fetch it separately so the error is recorded in the correct format.
      String serialization = null;
      try {
        serialization = systemDatabase.getWorkflowSerialization(workflowId);
      } catch (Exception ignored) {
        // Best-effort; fall through with null to use default serializer
      }
      persistWorkflowError(
          workflowId,
          new RuntimeException("Failed to deserialize workflow inputs: " + e.getMessage(), e),
          serialization);
      throw new RuntimeException("Failed to load workflow " + workflowId, e);
    }

    if (status == null) {
      logger.error("Workflow not found {}", workflowId);
      throw new DBOSNonExistentWorkflowException(workflowId);
    }

    Object[] inputs = status.input();
    var wfName =
        RegisteredWorkflow.fullyQualifiedName(
            status.workflowName(), status.className(), status.instanceName());
    RegisteredWorkflow workflow = workflowMap.get(wfName);

    if (workflow == null) {
      throw new DBOSWorkflowFunctionNotFoundException(workflowId, wfName);
    }

    // Coerce deserialized arguments to match the method's expected parameter types.
    // Portable JSON deserializes to generic types (Integer, ArrayList, LinkedHashMap)
    // which may not match the method signature (long, String[], custom POJOs).
    // Other serializers may have similar issues (e.g., dates as strings).
    try {
      inputs = ArgumentCoercion.coerceArguments(inputs, workflow.workflowMethod());
    } catch (IllegalArgumentException e) {
      logger.error("Argument coercion failed for workflow {}", workflowId, e);
      persistWorkflowError(workflowId, e, status.serialization());
      throw e;
    }

    var options =
        new ExecutionOptions(workflowId, status.timeout(), status.deadline())
            .withSerialization(status.serialization());
    if (isRecoveryRequest) options = options.asRecoveryRequest();
    if (isDequeuedRequest) options = options.asDequeuedRequest();
    return executeWorkflow(workflow, inputs, options, null);
  }

  private record TimeoutAndDeadline(Duration timeout, Instant deadline) {}

  private static TimeoutAndDeadline resolveTimeoutAndDeadline(
      DBOSContext ctx, Timeout nextTimeout, Instant nextDeadline) {
    Duration timeout = ctx.getTimeout();
    Instant deadline = ctx.getDeadline();
    if (nextDeadline != null) {
      deadline = nextDeadline;
    } else if (nextTimeout instanceof Timeout.None) {
      timeout = null;
      deadline = null;
    } else if (nextTimeout instanceof Timeout.Explicit e) {
      timeout = e.value();
      deadline = Instant.ofEpochMilli(System.currentTimeMillis() + e.value().toMillis());
    }
    return new TimeoutAndDeadline(timeout, deadline);
  }

  // helper workflow execution methods
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

  private void validateWorkflow(String workflowName, String className) {
    validateWorkflow(workflowName, className, "");
  }

  private void validateWorkflow(String workflowName, String className, String instanceName) {
    var fqName = RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName);
    if (!workflowMap.containsKey(fqName)) {
      throw new IllegalStateException("Workflow function %s is not registered".formatted(fqName));
    }
  }

  private void validateQueue(String queueName) {
    if (queueName != null) {
      getQueue(queueName)
          .orElseThrow(
              () -> new IllegalStateException("Queue %s is not registered".formatted(queueName)));
    }
  }

  private void validateQueue(String queueName, String queuePartitionKey) {
    if (queueName == null || queueName.equals(Constants.DBOS_INTERNAL_QUEUE)) {
      if (queuePartitionKey != null) {
        throw new IllegalArgumentException(
            "DBOS internal queue is not a partitioned queue, but a partition key was provided");
      }
    } else {
      var queue = this.getQueue(queueName);
      if (queue.isPresent()) {
        if (queue.get().partitioningEnabled() && queuePartitionKey == null) {
          throw new IllegalArgumentException(
              "queue %s partitions enabled, but no partition key was provided"
                  .formatted(queueName));
        }

        if (!queue.get().partitioningEnabled() && queuePartitionKey != null) {
          throw new IllegalArgumentException(
              "queue %s is not a partitioned queue, but a partition key was provided"
                  .formatted(queueName));
        }
      } else {
        throw new IllegalArgumentException("queue %s does not exist".formatted(queueName));
      }
    }
  }

  // execute or enqueue a worklow
  private <T, E extends Exception> WorkflowHandle<T, E> executeWorkflow(
      RegisteredWorkflow workflow, Object[] args, ExecutionOptions options, WorkflowInfo parent) {

    Objects.requireNonNull(options, "options must not be null");
    var workflowId = Objects.requireNonNull(options.workflowId(), "workflowId must not be null");
    if (workflowId.isEmpty()) {
      throw new IllegalArgumentException("workflowId cannot be empty");
    }

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

    if (options.queueName() != null) {

      validateQueue(options.queueName(), options.queuePartitionKey());

      enqueueWorkflow(
          workflow.workflowName(),
          workflow.className(),
          workflow.instanceName(),
          maxRetries,
          args,
          null,
          options,
          parent,
          executorId(),
          appVersion(),
          appId(),
          systemDatabase,
          this.serializer);
      return new WorkflowHandleDBPoll<>(this, workflowId);
    }

    var badOptionList = new ArrayList<String>();
    if (options.deduplicationId() != null) {
      badOptionList.add("deduplicationId");
    }

    if (options.priority() != null) {
      badOptionList.add("priority");
    }

    if (options.queuePartitionKey() != null) {
      badOptionList.add("queuePartitionKey");
    }

    if (options.delay() != null) {
      badOptionList.add("delay");
    }

    if (!badOptionList.isEmpty()) {
      throw new IllegalArgumentException(
          "%s invalid options without a queue name: %s"
              .formatted(workflow.fullyQualifiedName(), String.join(", ", badOptionList)));
    }

    logger.debug("executeWorkflow {}({}) {}", workflow.fullyQualifiedName(), args, options);

    WorkflowInitResult initResult =
        persistWorkflow(
            systemDatabase,
            workflow.workflowName(),
            workflow.className(),
            workflow.instanceName(),
            maxRetries,
            args,
            null,
            workflowId,
            null,
            null,
            null,
            null,
            null,
            executorId(),
            // executed workflows always use the current app version.
            // Option.appVersion is only used for enqueue
            appVersion(),
            appId(),
            parent,
            options.timeoutDuration(),
            options.deadline(),
            options.isRecoveryRequest(),
            options.isDequeuedRequest(),
            options.serialization(),
            this.serializer);
    if (!initResult.shouldExecuteOnThisExecutor()) {
      return retrieveWorkflow(workflowId);
    }
    if (initResult.status().equals(WorkflowState.SUCCESS)) {
      return retrieveWorkflow(workflowId);
    } else if (initResult.status().equals(WorkflowState.ERROR)) {
      logger.warn("Idempotency check not impl for error");
    } else if (initResult.status().equals(WorkflowState.CANCELLED)) {
      logger.warn("Idempotency check not impl for cancelled");
    }

    final var finalOptions = options;
    Supplier<T> task =
        () -> {
          DBOSContextHolder.clear();
          var res = workflowsInProgress.putIfAbsent(workflowId, true);
          if (res != null) throw new DBOSWorkflowExecutionConflictException(workflowId);
          try {
            logger.debug(
                "executeWorkflow task {}({}) {}",
                workflow.fullyQualifiedName(),
                args,
                finalOptions);

            DBOSContextHolder.set(
                new DBOSContext(
                    workflowId,
                    parent,
                    finalOptions.timeoutDuration(),
                    finalOptions.deadline(),
                    SerializationUtil.PORTABLE.equals(initResult.serialization())
                        ? SerializationStrategy.PORTABLE
                        : SerializationStrategy.DEFAULT));

            if (Thread.currentThread().isInterrupted()) {
              logger.debug("executeWorkflow task interrupted before workflow.invoke");
              return null;
            }

            T output = workflow.invoke(args);

            if (Thread.currentThread().isInterrupted()) {
              logger.debug("executeWorkflow task interrupted before workflow.invoke completion");
              return null;
            }

            persistWorkflowOutput(workflowId, output, initResult.serialization());

            return output;
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

            logger.error("executeWorkflow {}", workflowId, actual);

            if (actual instanceof InterruptedException
                || actual instanceof DBOSWorkflowCancelledException) {
              throw new DBOSAwaitedWorkflowCancelledException(workflowId);
            }

            persistWorkflowError(workflowId, actual, initResult.serialization());
            throw e;
          } finally {
            DBOSContextHolder.clear();
            workflowsInProgress.remove(workflowId);
          }
        };

    if (initResult.deadlineEpochMS() != null
        && System.currentTimeMillis() > initResult.deadlineEpochMS()) {
      systemDatabase.cancelWorkflows(List.of(workflowId));
      return retrieveWorkflow(workflowId);
    }

    var future = CompletableFuture.supplyAsync(task, executorService);
    if (initResult.deadlineEpochMS() != null) {
      timeoutScheduler.schedule(
          () -> {
            if (!future.isDone()) {
              systemDatabase.cancelWorkflows(List.of(workflowId));
              future.cancel(true);
            }
          },
          initResult.deadlineEpochMS() - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS);
    }

    return new WorkflowHandleFuture<>(this, workflowId, future);
  }

  // Persist an queued workflow to the database without execution
  // used by DBOSExecutor and DBOSClient
  public static void enqueueWorkflow(
      String workflowName,
      String className,
      String instanceName,
      Integer maxRetries,
      Object[] positionalArgs,
      // Note, namedArgs param is to enable portable workflow enqueue via DBOSClient.
      // Typical Java workflow invocations do not use named args
      Map<String, Object> namedArgs,
      ExecutionOptions options,
      WorkflowInfo parent,
      String executorId,
      String appVersion,
      String appId,
      SystemDatabase systemDatabase,
      DBOSSerializer serializer) {

    if (Objects.requireNonNull(options.workflowId(), "workflowId must not be null").isEmpty()) {
      throw new IllegalArgumentException("workflowId cannot be empty");
    }
    if (Objects.requireNonNull(options.queueName(), "queueName must not be null").isEmpty()) {
      throw new IllegalArgumentException("queueName cannot be empty");
    }
    if (options.queuePartitionKey() != null && options.deduplicationId() != null) {
      throw new IllegalArgumentException(
          "queuePartitionKey and deduplicationId cannot both be set");
    }

    logger.debug(
        "enqueueWorkflow {}:{}:{}",
        options.workflowId(),
        options.queueName(),
        RegisteredWorkflow.fullyQualifiedName(
            workflowName, Objects.requireNonNullElse(className, ""), instanceName));

    // Note, options.appVesion specifies the appVersion the workflow starter may have set
    // app version parameter specifies the current running code app version, if known.
    // options.appVersion takes presidence if specified
    if (options.appVersion() != null) {
      appVersion = options.appVersion();
    }

    try {
      persistWorkflow(
          systemDatabase,
          workflowName,
          className,
          instanceName,
          maxRetries,
          positionalArgs,
          namedArgs,
          options.workflowId(),
          options.queueName(),
          options.deduplicationId(),
          options.priority(),
          options.queuePartitionKey(),
          options.delay(),
          executorId,
          appVersion,
          appId,
          parent,
          options.timeoutDuration(),
          options.deadline(),
          options.isRecoveryRequest(),
          options.isDequeuedRequest(),
          options.serialization(),
          serializer);
    } catch (DBOSWorkflowExecutionConflictException e) {
      logger.debug("Workflow execution conflict for workflowId {}", options.workflowId());
    } catch (DBOSQueueDuplicatedException e) {
      logger.debug(
          "Workflow queue {} reused deduplicationId {}", e.queueName(), e.deduplicationId());
      throw e;
    } catch (Throwable e) {
      var actual = (e instanceof InvocationTargetException ite) ? ite.getTargetException() : e;
      logger.error("enqueueWorkflow", actual);
      throw e;
    }
  }

  // save workflow metadata to the system database
  private static WorkflowInitResult persistWorkflow(
      SystemDatabase systemDatabase,
      String workflowName,
      String className,
      String instanceName,
      Integer maxRetries,
      Object[] positionalArgs,
      // Note, namedArgs param is to enable portable workflow enqueue via DBOSClient.
      // Typical Java workflow invocations do not use named args
      Map<String, Object> namedArgs,
      String workflowId,
      String queueName,
      String deduplicationId,
      Integer priority,
      String queuePartitionKey,
      Duration delay,
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

    // Serialize inputs using the specified serialization format
    var serializedArgs =
        SerializationUtil.serializeArgs(
            Objects.requireNonNullElseGet(positionalArgs, () -> new Object[0]),
            namedArgs,
            serialization,
            serializer);
    String inputString = serializedArgs.serializedValue();
    String actualSerialization = serializedArgs.serialization();
    var startTime = System.currentTimeMillis();

    Instant effectiveDeadline = (queueName != null && timeout != null) ? null : deadline;

    final int retries = maxRetries == null ? Constants.DEFAULT_MAX_RECOVERY_ATTEMPTS : maxRetries;
    WorkflowStatusInternal workflowStatusInternal =
        new WorkflowStatusInternal(
            workflowId,
            workflowName,
            className,
            instanceName,
            queueName,
            deduplicationId,
            priority,
            queuePartitionKey,
            delay,
            null,
            null,
            null,
            inputString,
            executorId,
            appVersion,
            appId,
            timeout,
            effectiveDeadline,
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

  private void persistWorkflowOutput(String workflowId, Object result, String serialization) {
    var serialized = SerializationUtil.serializeValue(result, serialization, this.serializer);
    systemDatabase.recordWorkflowOutput(workflowId, serialized.serializedValue());
  }

  private void persistWorkflowError(String workflowId, Throwable error, String serialization) {
    var serialized = SerializationUtil.serializeError(error, serialization, this.serializer);
    systemDatabase.recordWorkflowError(workflowId, serialized.serializedValue());
  }
}
