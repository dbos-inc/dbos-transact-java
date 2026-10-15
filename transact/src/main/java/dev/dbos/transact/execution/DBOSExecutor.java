package dev.dbos.transact.execution;

import dev.dbos.transact.AlertHandler;
import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.EnqueueOptions;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.admin.AdminServer;
import dev.dbos.transact.conductor.Conductor;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.context.WorkflowInfo;
import dev.dbos.transact.database.ChildOf;
import dev.dbos.transact.database.DebounceCaller;
import dev.dbos.transact.database.ExternalState;
import dev.dbos.transact.database.GetEventCaller;
import dev.dbos.transact.database.Result;
import dev.dbos.transact.database.SendCaller;
import dev.dbos.transact.database.StreamIterator;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.database.TimedWorkflowStatus;
import dev.dbos.transact.database.WorkflowInitResult;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.exceptions.DBOSUnexpectedStepException;
import dev.dbos.transact.exceptions.DBOSWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException;
import dev.dbos.transact.internal.AppVersionComputer;
import dev.dbos.transact.internal.StepCheckpointStore;
import dev.dbos.transact.internal.Validation;
import dev.dbos.transact.internal.WorkflowRegistry;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.JsonUtility;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.DebounceResult;
import dev.dbos.transact.workflow.Debouncer.DebounceIds;
import dev.dbos.transact.workflow.ForkFromFailureOptions;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueConflictResolution;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.RewindOptions;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.SendMessage;
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
import dev.dbos.transact.workflow.internal.InternalWorkflows;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;
import dev.dbos.transact.workflow.internal.WorkflowHandleFuture;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.cronutils.model.time.ExecutionTime;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSExecutor implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(DBOSExecutor.class);

  // Invocation and hookHolder are used by startWorkflow to capture
  // workflow information w/o executing workflow function

  public record Invocation(
      DBOSExecutor executor,
      String workflowName,
      String className,
      String instanceName,
      Object[] args) {

    public String fqName() {
      return RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName);
    }
  }

  private record QueueBucket(String queueName, String partitionKey) {}

  // One execution of a workflow. A workflow resumed or recovered while a run of it is still going
  // here is dispatched here again under a new token, and the two coexist until the stale one
  // reaches its next checkpoint and stops; keyed by workflow ID alone, the new run would be turned
  // away, and both would wait on each other forever.
  private record ActiveExecution(String workflowId, String ownerXid) {}

  // Owns this executor's active entry for one execution of a workflow. Construction acquires the
  // entry (throwing on conflict); release is idempotent, so the try-with-resources backstop never
  // removes an entry acquired again after an explicit release().
  private final class ActiveWorkflowGuard implements AutoCloseable {
    private final AtomicBoolean released = new AtomicBoolean(false);
    private final ActiveExecution execution;

    ActiveWorkflowGuard(String workflowId, String ownerXid, QueueBucket bucket) {
      var execution = new ActiveExecution(workflowId, ownerXid);
      if (activeWorkflows.putIfAbsent(execution, bucket) != null) {
        throw new DBOSWorkflowExecutionConflictException(workflowId);
      }
      this.execution = execution;
    }

    // Releases the execution's queue slot. Runs before the outcome write, and before a run that
    // lost the workflow waits for the recorded outcome, so a waiting run holds no slot.
    void release() {
      if (released.compareAndSet(false, true)) {
        activeWorkflows.remove(execution);
      }
    }

    @Override
    public void close() {
      release();
    }
  }

  private static final ThreadLocal<Consumer<Invocation>> HOOK_HOLDER = new ThreadLocal<>();
  private static final QueueBucket NO_QUEUE = new QueueBucket(null, null);

  private final DBOSConfig config;
  private final DBOSSerializer serializer;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final ConcurrentHashMap<ActiveExecution, QueueBucket> activeWorkflows =
      new ConcurrentHashMap<>();

  private final boolean dbosCloud;
  private final String appId;
  private final String appName;
  private String appVersion;
  private String executorId;

  private Set<DBOSLifecycleListener> listeners;
  private Map<String, RegisteredWorkflow> workflowMap;
  private Map<String, RegisteredWorkflowInstance> instanceMap;
  private Map<String, RegisteredWorkflow> internalWorkflowMap;
  private Map<String, Queue> queueMap;

  private SystemDatabase systemDatabase;
  private QueueService queueService;
  private SchedulerService schedulerService;
  private AdminServer adminServer;
  private Conductor conductor;
  private ExecutorService executorService;
  private ScheduledExecutorService timeoutScheduler;
  private AlertHandler alertHandler;
  private Set<StepCheckpointStore> stepCheckpointStores = Set.of();
  private boolean keepCompletedStepCheckpoints = false;
  // How often the system database's identity is read again; see
  // SystemDatabase.refreshDatabaseIdentity.
  private static final Duration DATABASE_IDENTITY_REFRESH = Duration.ofMinutes(1);

  public DBOSExecutor(DBOSConfig config) {
    this.config = config;
    this.serializer = config.serializer();

    if (config.conductorKey() != null && config.executorId() != null) {
      throw new IllegalArgumentException(
          "DBOSConfig.executorId cannot be specified when using Conductor");
    }

    dbosCloud = Boolean.parseBoolean(System.getenv("DBOS__CLOUD"));
    appId = Objects.requireNonNullElse(System.getenv("DBOS__APPID"), "");
    appName =
        dbosCloud
            ? Objects.requireNonNullElse(System.getenv("DBOS_APP_NAME"), "")
            : config.appName();
    appVersion = Objects.requireNonNullElse(System.getenv("DBOS__APPVERSION"), "");
    var envExecutorId = System.getenv("DBOS__VMID");
    executorId = envExecutorId == null || envExecutorId.isEmpty() ? "local" : envExecutorId;

    if (appName.isEmpty()) {
      var msg =
          dbosCloud
              ? "DBOS_APP_NAME environment variable must be set when DBOS__CLOUD is true"
              : "DBOSConfig.appName field must not be empty";
      throw new IllegalArgumentException(msg);
    }

    // Nothing in Transact needs the name to look like anything: it is a bound parameter and a
    // hash input, never an identifier. Conductor does -- it addresses the application by name in
    // its websocket URL, and registers no name outside its rule. So an application that is about
    // to connect cannot usefully launch, while a self-hosted one is told and left alone; rejecting
    // that one would break applications already running under such a name for no reason Transact
    // can point at.
    if (!Validation.isValidApplicationName(appName)) {
      var msg = Validation.applicationNameNotAcceptedByConductor("application name", appName);
      if (dbosCloud || config.conductorKey() != null) {
        throw new IllegalArgumentException(msg);
      }
      logger.warn(msg);
    }

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
      if (config.conductorKey() != null) {
        executorId = UUID.randomUUID().toString();
      }
    }
  }

  @SuppressWarnings("removal")
  public void start(
      DBOS dbos,
      Set<DBOSLifecycleListener> listenerSet,
      Map<String, RegisteredWorkflow> workflowMap,
      Map<String, RegisteredWorkflowInstance> instanceMap,
      Map<String, RegisteredWorkflow> internalWorkflowMap,
      Map<String, RegisteredWorkflowInstance> internalInstanceMap,
      List<Queue> queues,
      Set<StepCheckpointStore> stepCheckpointStores,
      boolean keepCompletedStepCheckpoints,
      AlertHandler alertHandler) {

    if (isRunning.compareAndSet(false, true)) {
      logger.info("DBOS Executor starting");

      this.workflowMap = workflowMap;
      this.instanceMap = instanceMap;
      this.internalWorkflowMap = internalWorkflowMap;
      this.queueMap =
          queues.stream().collect(Collectors.toUnmodifiableMap(Queue::name, queue -> queue));
      this.listeners = listenerSet;
      this.stepCheckpointStores = stepCheckpointStores;
      this.keepCompletedStepCheckpoints = keepCompletedStepCheckpoints;
      this.alertHandler = alertHandler;

      if (this.appVersion == null || this.appVersion.isEmpty()) {
        // The resolved name, not config.appName(): on DBOS Cloud the executor takes its name
        // from DBOS_APP_NAME, and the version hash must key on the same identity that owns the
        // rows and that the peer-ownership checks compare against.
        this.appVersion =
            AppVersionComputer.computeAppVersion(
                DBOS.version(), this.appName, workflowMap.values());
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

      systemDatabase = SystemDatabase.create(config, this.executorId, this.appName);
      systemDatabase.start();
      // Handed to the executor service: a refresh waits for a pool connection, and this scheduler
      // also fires workflow timeouts.
      timeoutScheduler.scheduleWithFixedDelay(
          () -> executorService.submit(systemDatabase::refreshDatabaseIdentity),
          DATABASE_IDENTITY_REFRESH.toSeconds(),
          DATABASE_IDENTITY_REFRESH.toSeconds(),
          TimeUnit.SECONDS);

      systemDatabase.createApplicationVersion(this.appVersion, null);
      var latest = systemDatabase.getLatestApplicationVersion();
      if (!latest.versionName().equals(this.appVersion)) {
        logger.warn(
            "Current version {} is not the latest version. Latest version is {}",
            this.appVersion,
            latest.versionName());
      }

      // Conductor decides which executors are gone and issues the recovery itself, so a sweep
      // here would race that decision.
      //
      // Otherwise sweep before the queue runner starts, so recovered work is not racing a dequeue
      // pass already in flight, and before launch returns, so a workflow the application starts
      // the instant it does cannot be seen by the sweep -- such a row is PENDING under this
      // executor's id too, and indistinguishable from an abandoned one.
      if (dbosCloud || config.conductorKey() != null) {
        logger.debug("Skipping executor self recovery: recovery is managed by Conductor");
      } else {
        recoverPendingWorkflows(List.of(executorId()));
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

      if (dbosCloud) {
        String cloudAppName = System.getenv("DBOS__CONDUCTOR_APP_NAME");
        String cloudKey = System.getenv("DBOS__CONDUCTOR_KEY");
        String cloudUrl = System.getenv("DBOS__CONDUCTOR_URL");
        if (cloudAppName != null
            && !cloudAppName.isBlank()
            && cloudKey != null
            && !cloudKey.isBlank()
            && cloudUrl != null
            && !cloudUrl.isBlank()) {
          logger.debug("Starting Conductor connection (DBOS Cloud)");
          conductor =
              new Conductor.Builder(this, systemDatabase, cloudKey)
                  .appName(cloudAppName)
                  .domain(cloudUrl)
                  .build();
          conductor.start();
        }
      } else {
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
      }

      if (dbosCloud || config.adminServer()) {
        try {
          adminServer =
              new AdminServer(dbosCloud ? 3001 : config.adminServerPort(), this, systemDatabase);
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
      this.internalWorkflowMap = null;

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

  /**
   * Extends a debounced DELAYED instance of {@code workflow}'s delay and replaces its inputs with
   * {@code args} in the workflow's registered format, or reports who holds the pair instead.
   *
   * <p>With a {@code stepName}, and called from a workflow, it is that step: replay returns what
   * the step recorded, and a first run bounces and checkpoints in one transaction. {@code ids},
   * when given, are returned completed with the outcome and, when nothing was extended, with any
   * debouncer workflow of this application holding the key; see {@link
   * dev.dbos.transact.database.dao.WorkflowDAO#debounceDelayedWorkflow}. Outside a workflow, or
   * with no step name, it is the plain bounce.
   *
   * <p>Returns what the step records as {@code Object}, deliberately: a replay hands back what the
   * serializer preserved, which under a custom serializer that drops Java types is a map, and the
   * caller adapts it rather than casting.
   */
  public Object debounceDelayedWorkflow(
      RegisteredWorkflow workflow,
      String queueName,
      String deduplicationId,
      long delayUntilEpochMs,
      Object[] args,
      @Nullable String stepName,
      @Nullable DebounceIds ids) {
    var ctx = DBOSContextHolder.get();
    DebounceCaller caller = null;
    if (stepName != null && ctx.isInWorkflow() && !ctx.isInStep()) {
      if (HOOK_HOLDER.get() != null) {
        throw new RuntimeException(
            "@Step functions cannot be called from the startWorkflow lambda");
      }
      caller =
          new DebounceCaller(
              ctx.getWorkflowId(), ctx.getAndIncrementFunctionId(), stepName, ctx.getOwnerXid());
    }
    return systemDatabase.debounceDelayedWorkflow(
        workflow.workflowName(),
        workflow.className(),
        workflow.instanceName(),
        queueName,
        deduplicationId,
        delayUntilEpochMs,
        args,
        workflow.serializationStrategy() != null
            ? workflow.serializationStrategy().formatName()
            : null,
        ids,
        caller);
  }

  /**
   * Enqueues {@code workflow} as a debounced workflow: DELAYED on {@code queueName} for {@code
   * delay}, capped at {@code debounceDeadline} if not null, holding {@code deduplicationId} as its
   * debounce key. Called from a workflow, it is a child enqueue, so a replay returns whatever child
   * that slot recorded -- which, for a debounce recorded before debounced workflows, is the
   * debouncer workflow, not {@code workflowId}.
   *
   * <p>An enqueue that collided records nothing in its slot, so a replay alone cannot tell it from
   * one that never ran. A collision is always followed by the {@code DBOS.lookupDebouncer} step in
   * the next slot, in this release and the ones before it, so when that step is recorded the replay
   * throws the collision again rather than enqueue for real: the key may be free by now, or held on
   * another queue than the one it collided on.
   *
   * <p>The workflow takes {@code timeout}, timed from its dequeue, and no deadline: it may start
   * long after the call, so neither the caller's deadline nor its timeout carries over. The
   * caller's authentication does.
   */
  public <T, E extends Exception> WorkflowHandle<T, E> enqueueDebounced(
      RegisteredWorkflow workflow,
      Object[] args,
      String workflowId,
      String queueName,
      String deduplicationId,
      Duration delay,
      @Nullable Instant debounceDeadline,
      @Nullable Integer priority,
      @Nullable String appVersion,
      @Nullable Duration timeout,
      @Nullable Map<String, Object> attributes) {
    var ctx = DBOSContextHolder.get();
    var parent = getParent(ctx);
    if (parent != null
        && systemDatabase.checkChildWorkflow(parent.workflowId(), parent.functionId()).isEmpty()
        && lookupDebouncerRecordedAfter(parent)) {
      throw new DBOSQueueDuplicatedException(workflowId, queueName, deduplicationId);
    }
    var options =
        debouncedOptions(
            ctx,
            workflowId,
            queueName,
            deduplicationId,
            delay,
            debounceDeadline,
            priority,
            appVersion,
            timeout,
            attributes);
    return executeWorkflow(workflow, args, options, parent);
  }

  /** The options {@link #enqueueDebounced} creates a debounced workflow with. */
  private static ExecutionOptions debouncedOptions(
      DBOSContext ctx,
      String workflowId,
      String queueName,
      String deduplicationId,
      Duration delay,
      @Nullable Instant debounceDeadline,
      @Nullable Integer priority,
      @Nullable String appVersion,
      @Nullable Duration timeout,
      @Nullable Map<String, Object> attributes) {
    return new ExecutionOptions(
            workflowId,
            Timeout.of(timeout),
            null,
            queueName,
            deduplicationId,
            priority,
            null,
            delay,
            appVersion,
            null)
        .withAuthenticatedUser(ctx.resolveNextAuthenticatedUser())
        .withAssumedRole(ctx.resolveNextAssumedRole())
        .withAuthenticatedRoles(ctx.resolveNextAuthenticatedRoles())
        .withAttributes(attributes)
        .withDebounce(debounceDeadline);
  }

  /**
   * Whether {@code slot} is an enqueue that collided when it first ran: nothing is recorded there,
   * and {@code DBOS.lookupDebouncer} is recorded in the slot after.
   */
  private boolean collidedEnqueueRecordedAt(WorkflowInfo slot) {
    try {
      if (systemDatabase.checkStepResult(
              slot.workflowId(), slot.functionId(), "DBOS.takeOverStrandedDebouncer", null)
          != null) {
        return false;
      }
    } catch (DBOSUnexpectedStepException e) {
      // Something else is recorded there; the takeover's own replay reports the mismatch.
      return false;
    }
    return lookupDebouncerRecordedAfter(slot);
  }

  /** Whether the slot after {@code parent}'s is a recorded {@code DBOS.lookupDebouncer} step. */
  private boolean lookupDebouncerRecordedAfter(WorkflowInfo parent) {
    try {
      return systemDatabase.checkStepResult(
              parent.workflowId(), parent.functionId() + 1, "DBOS.lookupDebouncer", null)
          != null;
    } catch (DBOSUnexpectedStepException e) {
      // Something else is recorded there; the enqueue's own replay reports the mismatch.
      return false;
    }
  }

  /**
   * The workflow holding {@code deduplicationId} on {@code queueName}, reported as a bounce that
   * extended nothing. Called from a workflow, it is the {@code DBOS.lookupDebouncer} step, recorded
   * one slot after the next: the slot between is where SDK versions before 1.2 tried to enqueue
   * their debouncer workflow before looking up the holder, and a collision there records nothing.
   * Keeping the gap lets a debounce such a version recorded replay here.
   *
   * <p>Returns what the step records as {@code Object}, as {@link #debounceDelayedWorkflow} does.
   */
  public Object findDeduplicationHolder(String queueName, String deduplicationId) {
    var ctx = DBOSContextHolder.get();
    if (ctx.isInWorkflow() && !ctx.isInStep()) {
      ctx.getAndIncrementFunctionId();
    }
    return runDbosFunctionAsStep(
        () ->
            (Object)
                new DebounceResult.NotBounced(
                    systemDatabase.findDeduplicationHolder(queueName, deduplicationId)),
        "DBOS.lookupDebouncer",
        null);
  }

  /**
   * {@link InternalWorkflows#takeOverStrandedDebouncer}, creating the promised workflow as {@link
   * #enqueueDebounced} would enqueue it under that id. Called from a workflow, it is the {@code
   * DBOS.takeOverStrandedDebouncer} step, checkpointed in the takeover's transaction with the
   * promised workflow as its child.
   */
  public @Nullable String takeOverStrandedDebouncer(
      RegisteredWorkflow workflow,
      Object[] args,
      String debouncerWorkflowId,
      String queueName,
      String deduplicationId,
      Duration delay,
      @Nullable Instant debounceDeadline,
      @Nullable Integer priority,
      @Nullable String appVersion,
      @Nullable Duration timeout,
      @Nullable Map<String, Object> attributes) {
    var ctx = DBOSContextHolder.get();
    DebounceCaller caller = null;
    if (ctx.isInWorkflow()) {
      // It creates a workflow, which a step cannot, as enqueueDebounced cannot.
      if (ctx.isInStep()) {
        throw new IllegalStateException("cannot invoke a workflow from a step");
      }
      if (HOOK_HOLDER.get() != null) {
        throw new RuntimeException(
            "@Step functions cannot be called from the startWorkflow lambda");
      }
      // A release before 1.2 never took over: on silence it tried to enqueue again, which
      // collided and recorded nothing, and looked the holder up in the next slot. Replaying that,
      // leave the slot to enqueueDebounced, which throws the collision again.
      if (collidedEnqueueRecordedAt(
          new WorkflowInfo(ctx.getWorkflowId(), ctx.getCurrentFunctionId(), ctx.getOwnerXid()))) {
        return null;
      }
      caller =
          new DebounceCaller(
              ctx.getWorkflowId(),
              ctx.getAndIncrementFunctionId(),
              "DBOS.takeOverStrandedDebouncer",
              ctx.getOwnerXid());
    }
    validateQueue(queueName, null);
    var parentWorkflowId = caller != null ? caller.workflowId() : null;
    return InternalWorkflows.takeOverStrandedDebouncer(
        systemDatabase,
        debouncerWorkflowId,
        promisedId -> {
          var options =
              debouncedOptions(
                  ctx,
                  promisedId,
                  queueName,
                  deduplicationId,
                  delay,
                  debounceDeadline,
                  priority,
                  appVersion,
                  timeout,
                  attributes);
          // The defaults executeWorkflow applies to an enqueue.
          if (workflow.serializationStrategy() != null) {
            options = options.withSerialization(workflow.serializationStrategy().formatName());
          }
          if (options.appVersion() == null) {
            options = options.withAppVersion(appVersion());
          }
          return workflowStatus(
              systemDatabase,
              workflow.workflowName(),
              workflow.className(),
              workflow.instanceName(),
              args,
              null,
              executorId(),
              appId(),
              parentWorkflowId,
              options,
              null);
        },
        caller);
  }

  QueueService getQueueService() {
    return queueService;
  }

  SchedulerService getSchedulerService() {
    return schedulerService;
  }

  // executor metadata

  public String appId() {
    return this.appId;
  }

  public String appName() {
    return appName;
  }

  public String appVersion() {
    return this.appVersion;
  }

  public String executorId() {
    return this.executorId;
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
      String workflowName, @Nullable String className, @Nullable String instanceName) {
    // Java workflows are always registered under a class, so a row enqueued without one (for a
    // target that looks workflows up by name alone, such as Python) can never match here.
    if (className == null) {
      return Optional.empty();
    }
    var fqName = RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName);
    var wf = this.workflowMap.get(fqName);
    if (wf == null) {
      wf = this.internalWorkflowMap.get(fqName);
    }
    return Optional.ofNullable(wf);
  }

  public Optional<Queue> findQueue(String queueName) {
    return findStaticQueue(queueName)
        .or(() -> queueService.findDynamicQueue(queueName))
        .or(() -> systemDatabase.findQueue(queueName));
  }

  public Collection<Queue> getStaticQueues() {
    return this.queueMap.values();
  }

  public Optional<Queue> findStaticQueue(String queueName) {
    return Optional.ofNullable(this.queueMap.get(queueName));
  }

  public void registerDynamicQueue(
      String name, QueueOptions options, QueueConflictResolution onConflict) {
    boolean updateExisting =
        switch (onConflict) {
          case ALWAYS_UPDATE -> true;
          case NEVER_UPDATE -> false;
          case UPDATE_IF_LATEST_VERSION ->
              appVersion.equals(systemDatabase.getLatestApplicationVersion().versionName());
        };
    // A runtime registers queues for itself: it is the process that will poll them.
    systemDatabase.upsertQueue(name, options, updateExisting, null);
  }

  public void updateDynamicQueue(String name, QueueOptions options) {
    systemDatabase.updateQueue(name, options);
  }

  public Optional<Queue> findDynamicQueue(String name) {
    return systemDatabase.findQueue(name);
  }

  public boolean deleteDynamicQueue(String name) {
    return systemDatabase.deleteQueue(name);
  }

  public List<Queue> listDynamicQueues() {
    return systemDatabase.listQueues();
  }

  public List<Queue> listDynamicQueues(@Nullable List<String> applicationName) {
    return systemDatabase.listQueues(applicationName);
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

  public long queueActiveCount(String queueName, String partitionKey) {
    var target = new QueueBucket(queueName, partitionKey);
    return activeWorkflows.values().stream().filter(target::equals).count();
  }

  /** Workflows this executor is running from {@code queueName}, across every partition of it. */
  public long queueActiveCount(String queueName) {
    return activeWorkflows.values().stream()
        .filter(bucket -> queueName.equals(bucket.queueName()))
        .count();
  }

  // DBOS / DBOSClient API methods

  private void sendBulkInternal(
      List<SendMessage> messages,
      boolean sendToForks,
      SerializationStrategy serialization,
      String functionName) {

    // The row is authoritative: a workflow running under a portable row writes its messages in
    // that format too, so a peer in another language can read them. setEvent and writeStream
    // already inherit it.
    if (serialization == null || serialization.equals(SerializationStrategy.DEFAULT)) {
      serialization =
          Objects.requireNonNullElse(
              DBOSContextHolder.get().getSerialization(), SerializationStrategy.DEFAULT);
    }

    sendBulkAs(messages, sendToForks, serialization.formatName(), functionName);
  }

  /**
   * Send with a resolved serialization format, where null means the application's configured
   * serializer.
   */
  private void sendBulkAs(
      List<SendMessage> messages,
      boolean sendToForks,
      @Nullable String serializationFormat,
      String functionName) {

    DBOSContext ctx = DBOSContextHolder.get();
    // A send from a workflow is its step; one from inside a step records nothing, but is still
    // made only while the step's execution owns the workflow.
    SendCaller caller = null;
    if (ctx.isInWorkflow()) {
      caller =
          ctx.isInStep()
              ? new SendCaller.FromStep(ctx.getWorkflowId(), ctx.getOwnerXid())
              : new SendCaller.FromWorkflow(
                  ctx.getWorkflowId(), ctx.getAndIncrementFunctionId(), ctx.getOwnerXid());
    }
    systemDatabase.sendBulk(messages, caller, functionName, sendToForks, serializationFormat);
  }

  /**
   * Send a message that DBOS itself reads back, such as the debouncer's control messages. It
   * carries a Java value to a Java workflow, so it takes the application's own serializer whatever
   * format the calling workflow runs under: inherit a portable one and the receiver is handed a Map
   * where it expects its own type.
   */
  public void sendInternal(
      String destinationId, Object message, String topic, String idempotencyKey) {
    sendBulkAs(
        List.of(new SendMessage(destinationId, message, topic, idempotencyKey)),
        false,
        null,
        "DBOS.send");
  }

  public void sendBulk(
      List<SendMessage> messages, boolean sendToForks, SerializationStrategy serialization) {
    sendBulkInternal(messages, sendToForks, serialization, "DBOS.sendBulk");
  }

  public void send(
      String destinationId,
      Object message,
      String topic,
      String idempotencyKey,
      SerializationStrategy serialization,
      boolean sendToForks) {
    sendBulkInternal(
        List.of(new SendMessage(destinationId, message, topic, idempotencyKey)),
        sendToForks,
        serialization,
        "DBOS.send");
  }

  public void send(
      String destinationId,
      Object message,
      String topic,
      String idempotencyKey,
      SerializationStrategy serialization) {
    send(destinationId, message, topic, idempotencyKey, serialization, false);
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
        ctx.getWorkflowId(), stepFunctionId, timeoutFunctionId, topic, timeout, ctx.getOwnerXid());
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
        ctx.getWorkflowId(),
        stepId,
        key,
        value,
        asStep,
        serialization.formatName(),
        ctx.getOwnerXid());
  }

  public Object getEvent(String workflowId, String key, Duration timeout) {
    logger.debug("Received getEvent for {} {}", workflowId, key);

    DBOSContext ctx = DBOSContextHolder.get();

    if (ctx.isInWorkflow() && !ctx.isInStep()) {
      int stepId = ctx.getAndIncrementFunctionId();
      int timeoutStepId = ctx.getAndIncrementFunctionId();
      GetEventCaller caller =
          new GetEventCaller(ctx.getWorkflowId(), stepId, timeoutStepId, ctx.getOwnerXid());
      return systemDatabase.getEvent(workflowId, key, timeout, caller);
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

    systemDatabase.sleep(
        context.getWorkflowId(),
        context.getAndIncrementFunctionId(),
        duration,
        context.getOwnerXid());
  }

  public void cancelWorkflows(List<String> workflowIds, boolean cancelChildren) {
    Objects.requireNonNull(workflowIds);

    this.runDbosFunctionAsStep(
        () -> {
          logger.info(
              "Cancelling workflow(s) {}{}",
              workflowIds,
              cancelChildren ? " and their children" : "");
          systemDatabase.cancelWorkflows(workflowIds, cancelChildren);
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

  public void updateWorkflowAttributes(String workflowId, Map<String, Object> attributes) {
    Objects.requireNonNull(workflowId);
    this.runDbosFunctionAsStep(
        () -> {
          logger.info("Updating attributes of workflow: {}", workflowId);
          systemDatabase.updateWorkflowAttributes(workflowId, attributes);
          return null; // void
        },
        "DBOS.updateWorkflowAttributes",
        null);
  }

  /** Whether any transactional step factory has registered its checkpoints. */
  public boolean hasStepCheckpointStores() {
    return !stepCheckpointStores.isEmpty();
  }

  /**
   * Deletes a workflow's checkpoints from {@code fromStepId} on, in every registered transactional
   * step factory. Each store commits its own delete, so a failure part way leaves the stores before
   * it cleared; deleting again is safe.
   */
  public void deleteStepCheckpoints(String workflowId, int fromStepId) {
    for (var store : stepCheckpointStores) {
      try {
        store.deleteCheckpoints(workflowId, fromStepId);
      } catch (SQLException | RuntimeException e) {
        throw new RuntimeException(
            "Failed to delete the transactional step checkpoints of workflow " + workflowId, e);
      }
    }
  }

  /**
   * Deletes every checkpoint of the given workflows, in every registered transactional step
   * factory. As with {@link #deleteStepCheckpoints(String, int)}, a failure part way leaves the
   * stores before it cleared, and deleting again is safe.
   */
  public void deleteStepCheckpoints(Collection<String> workflowIds) {
    for (var store : stepCheckpointStores) {
      try {
        store.deleteCheckpoints(workflowIds);
      } catch (SQLException | RuntimeException e) {
        throw new RuntimeException(
            "Failed to delete the transactional step checkpoints of "
                + describeWorkflows(workflowIds),
            e);
      }
    }
  }

  // A retention batch can hold thousands of IDs, so name only the first few.
  private static String describeWorkflows(Collection<String> workflowIds) {
    var shown = workflowIds.stream().limit(5).toList();
    var more = workflowIds.size() - shown.size();
    return workflowIds.size()
        + " workflows "
        + shown
        + (more > 0 ? " and %d more".formatted(more) : "");
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

  public <T, E extends Exception> WorkflowHandle<T, E> rewindWorkflow(
      String workflowId, int startStep, RewindOptions options) {
    Objects.requireNonNull(workflowId);
    Objects.requireNonNull(options);

    this.runDbosFunctionAsStep(
        () -> {
          logger.info("Rewinding workflow: {} to step: {}", workflowId, startStep);

          validateQueue(options.queueName(), options.queuePartitionKey());

          // The step factories' checkpoints go first and the system database last. If a delete
          // fails, the workflow keeps its terminal status and is not re-enqueued, so nothing runs
          // against a partly cleared history, and since a delete is idempotent the rewind can
          // simply be retried. The checks run first so a running workflow keeps its checkpoints.
          if (hasStepCheckpointStores()) {
            systemDatabase.checkRewindable(workflowId, startStep);
            deleteStepCheckpoints(workflowId, startStep);
          }
          systemDatabase.rewindWorkflow(workflowId, startStep, options);
          return null; // void
        },
        "DBOS.rewindWorkflow",
        null);
    return retrieveWorkflow(workflowId);
  }

  public List<WorkflowHandle<Object, Exception>> forkFromFailure(
      List<String> workflowIds, ForkFromFailureOptions options) {

    List<String> forkedIds =
        this.runDbosFunctionAsStep(
            () -> {
              logger.info("Forking {} workflow(s) from failure", workflowIds.size());

              validateQueue(options.queueName(), options.queuePartitionKey());

              return systemDatabase.forkFromFailure(workflowIds, options);
            },
            "DBOS.forkFromFailure",
            null);
    return forkedIds.stream()
        .map(id -> (WorkflowHandle<Object, Exception>) retrieveWorkflow(id))
        .collect(Collectors.toList());
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
          ctx.getWorkflowId(),
          ctx.getCurrentFunctionId(),
          key,
          value,
          serialization.formatName(),
          ctx.getOwnerXid());
    } else {
      int functionId = ctx.getAndIncrementFunctionId();
      systemDatabase.writeStreamFromWorkflow(
          ctx.getWorkflowId(),
          functionId,
          key,
          value,
          serialization.formatName(),
          ctx.getOwnerXid());
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
    systemDatabase.closeStream(ctx.getWorkflowId(), functionId, key, ctx.getOwnerXid());
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
    systemDatabase.updateApplicationVersionTimestamp(versionName, Instant.now(), null);
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
    return listSchedules(statuses, workflowNames, namePrefixes, null);
  }

  public List<WorkflowSchedule> listSchedules(
      List<ScheduleStatus> statuses,
      List<String> workflowNames,
      List<String> namePrefixes,
      @Nullable List<String> applicationNames) {
    return this.runDbosFunctionAsStep(
        () -> systemDatabase.listSchedules(statuses, workflowNames, namePrefixes, applicationNames),
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

    var workflowIds = DBOSExecutor.backfillSchedule(scheduleName, start, end, systemDatabase);
    return workflowIds.stream().map(this::retrieveWorkflow).toList();
  }

  public static List<String> backfillSchedule(
      @NonNull String scheduleName,
      @NonNull Instant start,
      @NonNull Instant end,
      @NonNull SystemDatabase systemDatabase) {

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
          schedule.scheduleName(),
          systemDatabase);

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

    var workflowId = triggerSchedule(scheduleName, systemDatabase);
    return retrieveWorkflow(workflowId);
  }

  public static String triggerSchedule(
      @NonNull String scheduleName, @NonNull SystemDatabase systemDatabase) {
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
        schedule.scheduleName(),
        systemDatabase);
    return workflowId;
  }

  /**
   * The format a schedule's runs are recorded in: the application's own serializer, named
   * explicitly so that no path promotes the workflow's declared one over it. A schedule fires
   * inside one application, never across the enqueue or message boundary where interop happens, so
   * its runs take this whatever the workflow declares -- as in Python, TypeScript and Go.
   */
  private static String scheduledSerialization(@Nullable DBOSSerializer serializer) {
    return serializer != null ? serializer.name() : SerializationUtil.NATIVE;
  }

  private static void enqueueScheduledWorkflow(
      @NonNull String workflowName,
      @NonNull String className,
      @NonNull String workflowId,
      Object context,
      String queueName,
      @NonNull Instant scheduledAt,
      String scheduleName,
      SystemDatabase systemDatabase) {
    var latestAppVersion = systemDatabase.getLatestApplicationVersion().versionName();
    queueName = Objects.requireNonNullElse(queueName, Constants.DBOS_INTERNAL_QUEUE);
    var args = new Object[] {Objects.requireNonNull(scheduledAt), context};

    var options =
        new ExecutionOptions(workflowId)
            .withQueueName(queueName)
            .withAppVersion(latestAppVersion)
            .withScheduleName(scheduleName)
            .withSerialization(scheduledSerialization(systemDatabase.serializer()));
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
        null, // applicationName: this executor's own
        systemDatabase);
  }

  @SuppressWarnings("removal") // implements the deprecated ExternalState API
  public Optional<ExternalState> getExternalState(String service, String workflowName, String key) {
    return systemDatabase.getExternalState(service, workflowName, key);
  }

  @SuppressWarnings("removal") // implements the deprecated ExternalState API
  public ExternalState upsertExternalState(ExternalState state) {
    return systemDatabase.upsertExternalState(state);
  }

  public WorkflowStatus getWorkflowStatus(String workflowId) {
    return this.runDbosFunctionAsStep(
        () -> systemDatabase.getWorkflowStatus(workflowId), "DBOS.getWorkflowStatus", null);
  }

  public <T, E extends Exception> T getResult(String workflowId, boolean failIfMissing) throws E {
    return this.runDbosFunctionAsStep(
        () -> awaitWorkflowResult(workflowId, failIfMissing), "DBOS.getResult", workflowId);
  }

  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T getResult(String workflowId, Future<T> futureResult) throws E {
    return this.runDbosFunctionAsStep(
        () -> {
          try {
            return futureResult.get();
          } catch (DBOSWorkflowExecutionConflictException e) {
            return awaitWorkflowResult(workflowId, false);
          } catch (CancellationException e) {
            throw new DBOSAwaitedWorkflowCancelledException(workflowId);
          } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) {
              if (cause instanceof DBOSWorkflowExecutionConflictException) {
                return awaitWorkflowResult(workflowId, false);
              }
              if (cause instanceof DBOSWorkflowCancelledException cancelled) {
                throw new DBOSAwaitedWorkflowCancelledException(cancelled.workflowId());
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

  // A missing row normally means the workflow just hasn't been inserted yet (an unchecked
  // retrieve, or a debounced workflow whose row appears only after the debounce period), so
  // polling is correct. Callers that know the row must already exist (a run waiting for an
  // outcome it just failed to write) pass failIfMissing to fail fast instead of polling
  // forever.
  private <T, E extends Exception> T awaitWorkflowResult(String workflowId, boolean failIfMissing)
      throws E {
    var result = systemDatabase.<T>awaitWorkflowResult(workflowId, failIfMissing);
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
    var patched = systemDatabase.patch(workflowId, functionId, patchName, ctx.getOwnerXid());
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

  /**
   * Returns the given executors' abandoned workflows to their queues, and reports which moved.
   *
   * <p>Recovery re-enqueues rather than executing in this process, so every recovered workflow
   * starts through the queue's atomic ENQUEUED -> PENDING claim. That handoff admits exactly one
   * runner, which is what makes a duplicate recovery request cost nothing, and it lets the whole
   * fleet share a backlog instead of leaving it to the one executor that found it.
   *
   * <p>Workflow IDs rather than handles, because this process may run none of them.
   */
  public List<String> recoverPendingWorkflows(List<String> executorIds) {
    Objects.requireNonNull(executorIds);

    var recovered =
        systemDatabase.reenqueueForRecovery(executorIds, appVersion, Constants.DBOS_INTERNAL_QUEUE);
    if (recovered.isEmpty()) {
      logger.info("No workflows to recover from application version {}", appVersion);
    } else {
      logger.info(
          "Recovering {} workflow(s) from application version {}", recovered.size(), appVersion);
    }
    return recovered;
  }

  public void globalTimeout(Instant endTime) {
    var input =
        new ListWorkflowsInput()
            .withEndTime(endTime)
            .withStatus(
                List.of(WorkflowState.PENDING, WorkflowState.ENQUEUED, WorkflowState.DELAYED));
    for (WorkflowStatus status : systemDatabase.listWorkflows(input)) {
      cancelWorkflows(List.of(status.workflowId()), false);
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

  public <T, E extends Exception> T runDbosFunctionAsStep(
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

    var hook = HOOK_HOLDER.get();
    if (hook != null) {
      throw new RuntimeException("@Step functions cannot be called from the startWorkflow lambda");
    }

    var ctx = DBOSContextHolder.get();
    var workflowId = ctx.getWorkflowId();
    var stepId = ctx.getAndIncrementFunctionId();
    logger.debug("executeStep #{} ({}) for workflow {}", stepId, options.name(), workflowId);

    var prevResult =
        systemDatabase.checkStepResult(workflowId, stepId, options.name(), ctx.getOwnerXid());
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

      var shouldRetry = options.shouldRetry();
      if (exception != null && shouldRetry != null) {
        try {
          if (!shouldRetry.test(exception)) {
            break;
          }
        } catch (Exception predicateEx) {
          logger.warn(
              "shouldRetry predicate threw for step {} in workflow {}; treating as retry",
              options.name(),
              workflowId,
              predicateEx);
        }
      }

      try {
        Thread.sleep(retryInterval.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      retryInterval = Duration.ofNanos((long) (retryInterval.toNanos() * options.backOffRate()));
      curAttempts++;
    }

    // The step has run: a transactional step's own transaction has committed, and until its result
    // is recorded here its factory's checkpoint is the only record of it.
    boolean recorded = false;
    try {
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
        systemDatabase.recordStepResult(stepResult, startTime, ctx.getOwnerXid());
        recorded = true;
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
        systemDatabase.recordStepResult(stepResult, startTime, ctx.getOwnerXid());
        recorded = true;
        return output;
      }
    } finally {
      if (!recorded) {
        ctx.markStepResultUnrecorded();
      }
    }
  }

  /**
   * Throws {@link DBOSWorkflowExecutionConflictException} unless the execution running on this
   * thread still owns {@code workflowId}. Does nothing outside that workflow's execution.
   *
   * <p>For a transactional step, called inside its transaction on {@code conn}, drawn from {@code
   * dataSource} when the step's factory can say so. The check runs on {@code conn} and locks the
   * workflow's row until the step commits when {@code dataSource} is the system database's own
   * pool, or, when the factory cannot name its pool, when {@code conn} reaches the system
   * database's own database ({@link SystemDatabase#reachesThisDatabase}): taking a second
   * connection from the pool while holding one could exhaust it. Otherwise the read takes a system
   * database connection and no lock, as the step's database cannot hold the system database's lock
   * across its own commit. For a step whose factory cannot name its pool, that read waits for a
   * connection once, and otherwise throws {@link SystemDatabase.ConnectionUnavailableException}.
   * Ownership can then still move between this read and the commit; the step's result is the
   * owner's to replay, which holds because the step must be deterministic.
   */
  public void checkStillOwned(
      String workflowId, @Nullable DataSource dataSource, @Nullable Connection conn) {
    var ctx = DBOSContextHolder.get();
    var ownerXid = ctx.getOwnerXid();
    if (ownerXid == null || !workflowId.equals(ctx.getWorkflowId())) {
      return;
    }
    // On the step's own connection when it may come from the system database's pool: a step must
    // not ask that pool for a second connection while holding one. A factory that cannot name its
    // pool is asked whether its connection reaches the system database at all.
    if (conn != null
        && (dataSource != null
            ? systemDatabase.sharesPool(dataSource)
            : systemDatabase.reachesThisDatabase(conn))) {
      systemDatabase.checkOwner(conn, workflowId, ownerXid);
      return;
    }
    // A connection from a pool the factory cannot name may still be the system database's, its
    // identity not yet refreshed after a restart of the server. Waiting on the pool indefinitely
    // could then leave steps holding every connection, each waiting for one more, and the refresh
    // waiting with them: wait once, and on failure let the step release its connection, refresh
    // the identity, and run again.
    var owner =
        conn != null && dataSource == null
            ? systemDatabase.getWorkflowOwnerOnce(workflowId)
            : systemDatabase.getWorkflowOwner(workflowId);
    if (!ownerXid.equals(owner)) {
      throw DBOSWorkflowExecutionConflictException.notOwned(workflowId);
    }
  }

  /**
   * Reads the system database's identity again; see {@link SystemDatabase#refreshDatabaseIdentity}.
   */
  public void refreshDatabaseIdentity() {
    systemDatabase.refreshDatabaseIdentity();
  }

  /**
   * Deletes the transactional step checkpoints of the workflow whose execution on this thread has
   * just finished, in every step factory it ran steps through, before its outcome is recorded. The
   * system database records every one of those steps too, and that is what a later execution
   * replays, so the factories' rows are needed only while the workflow may still run them.
   *
   * <p>Each factory deletes and then checks that this execution still owns the workflow, in one
   * transaction; when it does not, the delete rolls back and the rest are left to the new owner.
   * Best effort: any other exception is logged and the next factory is still tried, and nothing
   * here changes the workflow's outcome. A row left behind is only replayed if the workflow runs
   * again. An {@link Error} is not caught; it propagates as it would from anywhere else in the run.
   */
  private void deleteCompletedStepCheckpoints(String workflowId) {
    if (keepCompletedStepCheckpoints) {
      return;
    }
    var ctx = DBOSContextHolder.get();
    if (ctx.hasUnrecordedStepResult()) {
      logger.warn(
          "Keeping the transactional step checkpoints of workflow {}: a step result was not recorded",
          workflowId);
      return;
    }
    for (var store : ctx.usedStepCheckpointStores()) {
      try {
        store.deleteCompletedCheckpoints(workflowId);
      } catch (Exception e) {
        if (isConflictFor(e, workflowId)) {
          return;
        }
        logger.warn(
            "Failed to delete the transactional step checkpoints of workflow {}", workflowId, e);
      }
    }
  }

  // Whether a conflict on this workflow is anywhere in the cause chain: workflow code may have
  // caught it and rethrown it wrapped, in a checked exception as well as an unchecked one.
  private static boolean isConflictFor(Throwable e, String workflowId) {
    var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
    for (Throwable t = e; t != null && seen.add(t); t = t.getCause()) {
      if (t instanceof DBOSWorkflowExecutionConflictException conflict
          && workflowId.equals(conflict.workflowId())) {
        return true;
      }
    }
    return false;
  }

  // Workflow Execution Methods

  // helper method to validate that @Workflow methods are not invoked from the startWorkflow lambda
  public void validateNonWorkflowInvocation() {
    var hook = HOOK_HOLDER.get();
    if (hook != null) {
      throw new RuntimeException("Only @Workflow functions may run in this context");
    }
  }

  // execute a workflow via a proxy
  public Object runWorkflow(
      Object target, String instanceName, Method method, Object[] args, Workflow wfTag)
      throws Exception {
    var className = WorkflowRegistry.getWorkflowClassName(target);
    var workflowName = WorkflowRegistry.getWorkflowName(wfTag, method);

    // If the hook holder is set, we're invoking the workflow from startWorkflow. In this case,
    // provide the workflow invocation info and return a default value without execution
    var hook = HOOK_HOLDER.get();
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

    // Looked up before the workflow ID is resolved, which takes the caller's next function ID and
    // any ID set with WorkflowOptions. A miss creates no workflow, so there is no ID to report.
    var workflow =
        getRegisteredWorkflow(workflowName, className, instanceName)
            .orElseThrow(
                () ->
                    new DBOSWorkflowFunctionNotFoundException(
                        null, workflowName, className, instanceName));

    var ctx = DBOSContextHolder.get();

    WorkflowInfo parent = getParent(ctx);
    String childWorkflowId =
        parent != null ? "%s-%d".formatted(parent.workflowId(), parent.functionId()) : null;

    var workflowId =
        Objects.requireNonNullElseGet(
            ctx.getNextWorkflowId(childWorkflowId), () -> UUID.randomUUID().toString());

    var td = ctx.resolveTimeoutAndDeadline();

    var options =
        new ExecutionOptions(workflowId, td.timeout(), td.deadline())
            .withAuthenticatedUser(ctx.resolveNextAuthenticatedUser())
            .withAssumedRole(ctx.resolveNextAssumedRole())
            .withAuthenticatedRoles(ctx.resolveNextAuthenticatedRoles())
            .withAttributes(ctx.resolveNextAttributes());
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

  /**
   * Capture the workflow invocation triggered by the supplied lambda without executing the
   * workflow. The lambda must call exactly one @Workflow method on a registered proxy on this
   * executor.
   */
  public <T, E extends Exception> Invocation captureInvocation(ThrowingSupplier<T, E> wfLambda) {
    AtomicReference<Invocation> capturedInvocation = new AtomicReference<>();
    DBOSExecutor.HOOK_HOLDER.set(
        (invocation) -> {
          if (!capturedInvocation.compareAndSet(null, invocation)) {
            throw new RuntimeException("Only one @Workflow can be called in the captured lambda");
          }
        });
    try {
      wfLambda.execute();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      DBOSExecutor.HOOK_HOLDER.remove();
    }
    var invocation =
        Objects.requireNonNull(
            capturedInvocation.get(), "The lambda must call exactly one @Workflow");
    if (invocation.executor() != this) {
      throw new IllegalStateException(
          "The @Workflow method must be called on the DBOS instance passed to the lambda");
    }
    return invocation;
  }

  // execute a workflow via startWorkflow
  public <T, E extends Exception> WorkflowHandle<T, E> startWorkflow(
      ThrowingSupplier<T, E> wfLambda, StartWorkflowOptions options) {

    logger.debug("startWorkflow {}", options);

    var invocation = captureInvocation(wfLambda);
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
    return startRegisteredWorkflow(workflow, args, options, null);
  }

  // overload used by SchedulerService: scheduleName is set only for schedule-triggered
  // executions, never by user-facing StartWorkflowOptions.
  @SuppressWarnings("removal") // honors the deprecated deadline option
  <T, E extends Exception> WorkflowHandle<T, E> startRegisteredWorkflow(
      RegisteredWorkflow workflow,
      Object[] args,
      StartWorkflowOptions options,
      String scheduleName) {
    var ctx = DBOSContextHolder.get();
    var parent = getParent(ctx);
    var childWorkflowId =
        parent != null ? "%s-%d".formatted(parent.workflowId(), parent.functionId()) : null;

    var td =
        options == null
            ? ctx.resolveTimeoutAndDeadline()
            : ctx.resolveTimeoutAndDeadline(options.timeout(), options.deadline());

    var workflowId =
        Objects.requireNonNullElseGet(
            options != null ? options.workflowId() : null,
            () ->
                Objects.requireNonNullElseGet(
                    ctx.getNextWorkflowId(childWorkflowId), () -> UUID.randomUUID().toString()));

    var execOptions =
        new ExecutionOptions(workflowId)
            .withOptions(options)
            .withTimeout(td.timeout())
            .withDeadline(td.deadline())
            .withAuthenticatedUser(
                options != null && options.authenticatedUser() != null
                    ? options.authenticatedUser()
                    : ctx.resolveNextAuthenticatedUser())
            .withAssumedRole(
                options != null && options.assumedRole() != null
                    ? options.assumedRole()
                    : ctx.resolveNextAssumedRole())
            .withAuthenticatedRoles(
                options != null && options.authenticatedRoles() != null
                    ? options.authenticatedRoles()
                    : ctx.resolveNextAuthenticatedRoles())
            .withAttributes(
                options != null && options.attributes() != null
                    ? options.attributes()
                    : ctx.resolveNextAttributes())
            .withScheduleName(scheduleName)
            .withSerialization(
                scheduleName == null ? null : scheduledSerialization(this.serializer));
    return executeWorkflow(workflow, args, execOptions, parent);
  }

  /**
   * Enqueue a workflow by name, without a reference to its function. The target may live in another
   * process or another language, so nothing here consults the local workflow registry and nothing
   * validates the queue against it.
   *
   * <p>Safe to call from inside a workflow: the enqueued workflow is recorded as a child, so a
   * replay after a crash returns a handle to the original rather than enqueueing a second one.
   *
   * <p>Unlike {@link #startRegisteredWorkflow}, the application version is left unset unless the
   * caller gives one. An unset version is only dequeued by an executor running the owning
   * application's latest registered version.
   */
  @SuppressWarnings("removal") // honors the deprecated deadline option
  public <T, E extends Exception> WorkflowHandle<T, E> enqueueWorkflowByName(
      EnqueueOptions options, Object[] positionalArgs, Map<String, Object> namedArgs) {

    Objects.requireNonNull(options, "options must not be null");

    var ctx = DBOSContextHolder.get();
    // Throws if called from a step, and takes the caller's next function ID when in a workflow.
    var parent = getParent(ctx);
    // In a workflow the derived ID makes a crash-replay collide with the original enqueue rather
    // than enqueueing a second workflow.
    var childWorkflowId =
        parent != null ? "%s-%d".formatted(parent.workflowId(), parent.functionId()) : null;
    var workflowId =
        Objects.requireNonNullElseGet(
            options.workflowId(),
            () ->
                Objects.requireNonNullElseGet(
                    ctx.getNextWorkflowId(childWorkflowId), () -> UUID.randomUUID().toString()));

    if (parent != null) {
      var childId = systemDatabase.checkChildWorkflow(parent.workflowId(), parent.functionId());
      if (childId.isPresent()) {
        return retrieveWorkflow(childId.get());
      }
    }

    // With neither a timeout nor a deadline given, the ambient ones apply, else the running
    // workflow's deadline, as startWorkflow does.
    var td = ctx.resolveTimeoutAndDeadline(options.timeout(), options.deadline());
    var execOptions =
        new ExecutionOptions(workflowId)
            .withOptions(options)
            .withTimeout(td.timeout())
            .withDeadline(td.deadline())
            .withAuthenticatedUser(
                options.authenticatedUser() != null
                    ? options.authenticatedUser()
                    : ctx.resolveNextAuthenticatedUser())
            .withAssumedRole(
                options.assumedRole() != null
                    ? options.assumedRole()
                    : ctx.resolveNextAssumedRole())
            .withAuthenticatedRoles(
                options.authenticatedRoles() != null
                    ? options.authenticatedRoles()
                    : ctx.resolveNextAuthenticatedRoles());

    enqueueWorkflow(
        options.workflowName(),
        options.className(),
        options.instanceName(),
        null, // maxRetries
        positionalArgs,
        namedArgs,
        execOptions,
        parent,
        executorId(),
        appId(),
        options.applicationName(),
        systemDatabase);

    return new WorkflowHandleDBPoll<>(this, workflowId);
  }

  /**
   * Runs the workflow described by a row the queue has just claimed with {@code ownerXid}.
   *
   * <p>The claim -- the queue's ENQUEUED -> PENDING transition -- wrote this workflow's status,
   * executor, deadline, attempt count and owner, so the run reads them back rather than writing
   * them again, and nothing here inserts a status row.
   *
   * <p>The run owns the workflow with {@code ownerXid}, and so does every write this makes before
   * the run starts, failures included: once a later claim has taken the workflow, none of them
   * lands.
   */
  public <T, E extends Exception> WorkflowHandle<T, E> executeWorkflowById(
      String workflowId, String ownerXid) {
    logger.debug("executeWorkflowById {}", workflowId);

    TimedWorkflowStatus timedStatus;
    try {
      timedStatus = systemDatabase.getTimedWorkflowStatus(workflowId);
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
          serialization,
          ownerXid);
      throw new RuntimeException("Failed to load workflow " + workflowId, e);
    }

    if (timedStatus == null) {
      logger.error("Workflow not found {}", workflowId);
      throw new DBOSNonExistentWorkflowException(workflowId);
    }
    var status = timedStatus.status();

    // The claim wrote this executor's id. A row naming another has been re-enqueued and taken by a
    // peer since -- a recovery request naming a live executor does that -- so this dispatch no
    // longer owns it. Checked before the error paths below, which would otherwise record a failure
    // on a row the peer is actively running.
    if (!executorId().equals(status.executorId())) {
      logger.warn(
          "Workflow {} is claimed by executor {}, not {}; not running it here",
          workflowId,
          status.executorId(),
          executorId());
      return new WorkflowHandleDBPoll<>(this, workflowId, true);
    }

    // Reading the row reports an unreadable payload as null, so running the workflow refuses for
    // itself: a workflow invoked with arguments it never had is worse than one marked ERROR.
    if (!SerializationUtil.canDeserialize(status.serialization(), serializer)) {
      var e =
          new IllegalStateException(
              "Cannot run workflow %s: its arguments are serialized as %s, which this application has no deserializer for"
                  .formatted(workflowId, status.serialization()));
      logger.error("Unreadable serialization for workflow {}", workflowId, e);
      // Recorded in this runtime's own format, not the row's: a format we cannot read is one
      // we cannot write either, and serializing the error into it would throw and leave the
      // workflow PENDING forever — the hang this refusal exists to prevent. The status is the
      // part that has to land.
      persistWorkflowError(workflowId, e, null, ownerXid);
      throw e;
    }

    Object[] inputs = status.input();
    RegisteredWorkflow workflow =
        getRegisteredWorkflow(status.workflowName(), status.className(), status.instanceName())
            .orElse(null);

    if (workflow == null) {
      throw new DBOSWorkflowFunctionNotFoundException(
          workflowId, status.workflowName(), status.className(), status.instanceName());
    }

    // Coerce deserialized arguments to match the method's expected parameter types.
    // Portable JSON deserializes to generic types (Integer, ArrayList, LinkedHashMap)
    // which may not match the method signature (long, String[], custom POJOs).
    // Other serializers may have similar issues (e.g., dates as strings).
    try {
      inputs = JsonUtility.coerceArguments(inputs, workflow.workflowMethod());
    } catch (IllegalArgumentException e) {
      logger.error("Argument coercion failed for workflow {}", workflowId, e);
      persistWorkflowError(workflowId, e, status.serialization(), ownerXid);
      throw e;
    }

    var options =
        new ExecutionOptions(workflowId, status.timeout(), status.deadline())
            .withSerialization(status.serialization())
            .withAuthenticatedUser(status.authenticatedUser())
            .withAssumedRole(status.assumedRole())
            .withAuthenticatedRoles(status.authenticatedRoles());
    return executeWorkflow(
        workflow, inputs, options.asClaimed(status, timedStatus.readAt(), ownerXid), null);
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
      return new WorkflowInfo(workflowId, functionId, ctx.getOwnerXid());
    }
    return null;
  }

  private void validateWorkflow(String workflowName, String className) {
    validateWorkflow(workflowName, className, "");
  }

  private void validateWorkflow(String workflowName, String className, String instanceName) {
    var fqName = RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName);
    if (!workflowMap.containsKey(fqName) && !internalWorkflowMap.containsKey(fqName)) {
      throw new IllegalStateException("Workflow function %s is not registered".formatted(fqName));
    }
  }

  private void validateQueue(String queueName) {
    if (queueName != null) {
      findQueue(queueName)
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
      var queue = findQueue(queueName);
      if (queue.isPresent()) {
        if (queue.get().isPartitioned() && queuePartitionKey == null) {
          throw new IllegalArgumentException(
              "queue %s partitions enabled, but no partition key was provided"
                  .formatted(queueName));
        }

        if (!queue.get().isPartitioned() && queuePartitionKey != null) {
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

    // Failing an explicit choice, the registration's declared format applies.
    if (options.serialization() == null) {
      if (workflow.serializationStrategy() != null) {
        options = options.withSerialization(workflow.serializationStrategy().formatName());
      }
    }

    Integer maxRetries = workflow.maxRecoveryAttempts() > 0 ? workflow.maxRecoveryAttempts() : null;

    if (options.queueName() != null && options.claimedStatus() == null) {
      // enqueue with the current app version if it's not explicitly set
      if (options.appVersion() == null) {
        options = options.withAppVersion(appVersion());
      }

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
          appId(),
          null, // applicationName: this executor's own
          systemDatabase);
      return new WorkflowHandleDBPoll<>(this, workflowId);
    }

    // executing workflows always use the current app version.
    options = options.withAppVersion(appVersion());

    if (options.claimedStatus() == null) {
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
    }

    logger.debug("executeWorkflow {}({}) {}", workflow.fullyQualifiedName(), args, options);

    var claimed = options.claimedStatus();
    WorkflowInitResult initResult;
    if (claimed != null) {
      // The claim counted this dispatch; dead-letter the workflow if that exhausted its attempts.
      // The unannotated default applies here as it did when the status upsert made this decision.
      int attempts = Objects.requireNonNullElse(claimed.recoveryAttempts(), 0);
      int retries = Objects.requireNonNullElse(maxRetries, Constants.DEFAULT_MAX_RECOVERY_ATTEMPTS);
      if (!claimed.status().equals(WorkflowState.SUCCESS)
          && !claimed.status().equals(WorkflowState.ERROR)
          && attempts > retries + 1) {
        systemDatabase.deadLetterWorkflows(List.of(workflowId), attempts);
        throw new DBOSMaxRecoveryAttemptsExceededException(workflowId, retries);
      }
      // Only a PENDING row owns its outcome. A row that moved on since the claim -- cancelled, or
      // finished by whoever held it before -- has nothing left for this run to do, and running it
      // would enter the workflow body, which no step guard stands in front of. The row was just
      // read, so failIfMissing: one deleted in the meantime raises rather than polling forever.
      if (!claimed.status().equals(WorkflowState.PENDING)) {
        return new WorkflowHandleDBPoll<>(this, workflowId, true);
      }
      // Deliberately no persistWorkflow: the claim already wrote the status, executor, deadline
      // and attempt count, and this row was read back from it, so re-upserting would only rewrite
      // what it just read. The claim is this run's title to the workflow, and the row is still
      // PENDING, so it executes.
      initResult =
          new WorkflowInitResult(
              claimed.status(),
              claimed.deadline(),
              true,
              claimed.serialization(),
              options.claimedAt(),
              options.claimedOwnerXid());
    } else {
      initResult =
          persistWorkflow(
              systemDatabase,
              workflow.workflowName(),
              workflow.className(),
              workflow.instanceName(),
              maxRetries,
              args,
              null,
              executorId(),
              appId(),
              parent,
              options,
              null); // applicationName: this executor's own
    }
    if (!initResult.shouldExecuteOnThisExecutor()) {
      return retrieveWorkflow(workflowId);
    }
    if (!initResult.status().equals(WorkflowState.PENDING)) {
      // This call created the row, but it has moved on since: a start whose acknowledgement was
      // lost finds it on the retry already completed, or cancelled by its deadline. There is
      // nothing left to run, and this call owns nothing; its result is the recorded outcome. The
      // row is known to have existed (persistWorkflow just read this status from it), so
      // failIfMissing: a row deleted in the meantime surfaces DBOSNonExistentWorkflowException
      // instead of polling forever.
      return new WorkflowHandleDBPoll<>(this, workflowId, true);
    }

    // The token this run owns the workflow with: the claim's, or the direct start's own.
    final String ownerXid = Objects.requireNonNull(initResult.ownerXid(), "ownerXid");
    final var finalOptions = options;
    final String notRecordedWarning =
        "Workflow outcome was not recorded: the workflow is no longer owned by this execution."
            + " Waiting for the recorded outcome.";
    Supplier<T> task =
        () -> {
          DBOSContextHolder.clear();
          var bucket =
              finalOptions.claimedStatus() != null
                  ? new QueueBucket(finalOptions.queueName(), finalOptions.queuePartitionKey())
                  : NO_QUEUE;
          // The warning logged when this run stops to adopt the recorded outcome, set by
          // whichever site found that this run does not own the workflow's outcome.
          String pendingAdopt;
          try (var active = new ActiveWorkflowGuard(workflowId, ownerXid, bucket)) {
            logger.debug(
                "executeWorkflow task {}({}) {}",
                workflow.fullyQualifiedName(),
                args,
                finalOptions);

            // A workflow's own writes inherit the format its row records. Only the two
            // built-in formats need naming: a custom serializer's name falls to DEFAULT,
            // which resolves to that same serializer.
            var serializationStrategy =
                SerializationUtil.PORTABLE.equals(initResult.serialization())
                    ? SerializationStrategy.PORTABLE
                    : SerializationUtil.NATIVE.equals(initResult.serialization())
                        ? SerializationStrategy.NATIVE
                        : SerializationStrategy.DEFAULT;

            DBOSContextHolder.set(
                new DBOSContext(
                    workflowId,
                    parent,
                    initResult.deadline(),
                    finalOptions.authenticatedUser(),
                    finalOptions.assumedRole(),
                    finalOptions.authenticatedRoles(),
                    serializationStrategy,
                    ownerXid));

            if (Thread.currentThread().isInterrupted()) {
              logger.debug("executeWorkflow task interrupted before workflow.invoke");
              return null;
            }

            T output = workflow.invoke(args);

            if (Thread.currentThread().isInterrupted()) {
              logger.debug("executeWorkflow task interrupted before workflow.invoke completion");
              return null;
            }

            deleteCompletedStepCheckpoints(workflowId);

            active.release();
            if (persistWorkflowOutput(workflowId, output, initResult.serialization(), ownerXid)) {
              return output;
            }
            // The row was not PENDING: this run no longer owns the workflow's outcome. It
            // may have been cancelled, dead-lettered, completed by a concurrent execution,
            // or handed back to the queue by a resume.
            pendingAdopt = notRecordedWarning;
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

            if (isConflictFor(e, workflowId)) {
              // This run no longer owns the workflow: a cancel, resume, recovery or another claim
              // took it, and one of its writes was refused. Never persist the conflict, even when
              // workflow code caught it and rethrew it wrapped: stop this run and adopt the
              // outcome the workflow records instead.
              pendingAdopt =
                  "Workflow is no longer owned by this execution. Waiting for the recorded outcome.";
            } else if (actual instanceof DBOSWorkflowCancelledException cancelled
                && cancelled.workflowId().equals(workflowId)) {
              // The run observed its own cancellation (checkWorkflow only throws this after
              // reading CANCELLED from the DB). Skip the outcome write so it can never clobber
              // the row, and adopt the recorded outcome: normally the row is still CANCELLED
              // and the wait throws DBOSAwaitedWorkflowCancelledException, but a concurrent
              // resume may have taken the workflow back, in which case the recorded outcome
              // is the truth.
              pendingAdopt =
                  "Workflow was cancelled during execution. Waiting for the recorded outcome.";
            } else {
              // The workflow's own error. The context is still this run's here: the finally
              // below clears it. An interrupted run keeps its checkpoints, as on return: the
              // error may be the interrupt's, cutting short a step result's write.
              if (!Thread.currentThread().isInterrupted()) {
                deleteCompletedStepCheckpoints(workflowId);
              }
              if (persistWorkflowError(workflowId, actual, initResult.serialization(), ownerXid)) {
                // active is already closed here: try-with-resources closes before catch runs,
                // so the entry is released before this terminal write becomes durable.
                logger.error("executeWorkflow {}", workflowId, actual);
                throw e;
              }
              // The row was not PENDING: this run no longer owns the workflow's outcome, and
              // the error it computed is not the workflow's error.
              logger.debug("executeWorkflow {}", workflowId, actual);
              pendingAdopt = notRecordedWarning;
            }
          } finally {
            DBOSContextHolder.clear();
          }

          // Reached only when a refusal above set pendingAdopt: this run does not own the
          // workflow's outcome. Stop executing it, wait for the recorded outcome, and deliver
          // that through this run's own future. The row is known to have existed (this run
          // inserted or read it), so failIfMissing: a missing row means it was deleted, and the
          // wait surfaces DBOSNonExistentWorkflowException instead of polling forever.
          logger.warn("{} workflowId {}", pendingAdopt, workflowId);
          return awaitWorkflowResult(workflowId, true);
        };

    // The deadline is on the database's clock, and so is the reading the row came back with, so
    // the time left is measured from that reading on this JVM's monotonic clock.
    var timeLeft =
        initResult.deadline() == null ? null : initResult.clock().until(initResult.deadline());
    // The run's own deadline cancels it only while this run owns the workflow: a timer outliving
    // a hand-off must not cancel the run that took over.
    if (timeLeft != null && (timeLeft.isNegative() || timeLeft.isZero())) {
      systemDatabase.cancelOwnedWorkflow(workflowId, ownerXid);
      return retrieveWorkflow(workflowId);
    }

    var future = CompletableFuture.supplyAsync(task, executorService);
    if (timeLeft != null) {
      timeoutScheduler.schedule(
          () -> {
            // A run that lost the workflow is left to reach the fence and report the owner's
            // outcome; cancelling its future would report a cancellation that never happened.
            if (!future.isDone() && systemDatabase.cancelOwnedWorkflow(workflowId, ownerXid)) {
              future.cancel(true);
            }
          },
          timeLeft.toMillis(),
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
      String appId,
      @Nullable String applicationName,
      SystemDatabase systemDatabase) {

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

    try {
      persistWorkflow(
          systemDatabase,
          workflowName,
          className,
          instanceName,
          maxRetries,
          positionalArgs,
          namedArgs,
          executorId,
          appId,
          parent,
          options,
          applicationName);
    } catch (DBOSQueueDuplicatedException e) {
      logger.debug(
          "Workflow queue {} reused deduplicationId {}", e.queueName(), e.deduplicationId());
      throw e;
    } catch (DBOSWorkflowExecutionConflictException e) {
      // A parent that no longer owns its workflow was refused: the fence working, not a failure.
      // The parent stops and waits for the recorded outcome.
      logger.debug("Child enqueue refused for workflowId {}", options.workflowId(), e);
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
      String executorId,
      String appId,
      WorkflowInfo parentWorkflow,
      ExecutionOptions options,
      @Nullable String applicationName) {

    var workflowStatusInternal =
        workflowStatus(
            systemDatabase,
            workflowName,
            className,
            instanceName,
            positionalArgs,
            namedArgs,
            executorId,
            appId,
            parentWorkflow != null ? parentWorkflow.workflowId() : null,
            options,
            applicationName);
    final int retries = maxRetries == null ? Constants.DEFAULT_MAX_RECOVERY_ATTEMPTS : maxRetries;
    if (parentWorkflow == null) {
      return systemDatabase.initWorkflowStatus(workflowStatusInternal, retries);
    }
    return systemDatabase.initChildWorkflowStatus(
        workflowStatusInternal,
        retries,
        new ChildOf(
            parentWorkflow.workflowId(),
            parentWorkflow.functionId(),
            workflowName,
            parentWorkflow.ownerXid()));
  }

  /**
   * The status row a workflow is created with, its inputs serialized. Used by {@link
   * #persistWorkflow}, and by a debouncer taking over a debouncer workflow, which creates the row
   * in its own transaction.
   */
  public static WorkflowStatusInternal workflowStatus(
      SystemDatabase systemDatabase,
      String workflowName,
      String className,
      String instanceName,
      Object[] positionalArgs,
      Map<String, Object> namedArgs,
      String executorId,
      String appId,
      @Nullable String parentWorkflowId,
      ExecutionOptions options,
      @Nullable String applicationName) {
    // The row is read back with the format it records, so the serializer is the one the system
    // database being written to was configured with -- never a second one passed in alongside it,
    // which could only match or be a bug.
    var serializer = systemDatabase.serializer();

    // Serialize inputs using the specified serialization format
    var serializedArgs =
        SerializationUtil.serializeArgs(
            Objects.requireNonNullElseGet(positionalArgs, () -> new Object[0]),
            namedArgs,
            options.serialization(),
            serializer);
    String inputString = serializedArgs.serializedValue();
    String actualSerialization = serializedArgs.serialization();

    Instant effectiveDeadline =
        (options.queueName() != null && options.timeoutDuration() != null)
            ? null
            : options.deadline();

    return new WorkflowStatusInternal(
        options.workflowId(),
        workflowName,
        className,
        instanceName,
        options.queueName(),
        options.deduplicationId(),
        options.priority(),
        options.queuePartitionKey(),
        options.delay(),
        options.authenticatedUser(),
        options.assumedRole(),
        options.authenticatedRoles(),
        inputString,
        executorId,
        options.appVersion(),
        appId,
        options.timeoutDuration(),
        effectiveDeadline,
        parentWorkflowId,
        actualSerialization,
        options.attributes(),
        options.scheduleName(),
        applicationName,
        options.isDebounced(),
        options.debounceDeadline());
  }

  private boolean persistWorkflowOutput(
      String workflowId, Object result, String serialization, String ownerXid) {
    var serialized = SerializationUtil.serializeValue(result, serialization, this.serializer);
    return systemDatabase.recordWorkflowOutput(workflowId, serialized.serializedValue(), ownerXid);
  }

  private boolean persistWorkflowError(
      String workflowId, Throwable error, String serialization, String ownerXid) {
    var serialized = SerializationUtil.serializeError(error, serialization, this.serializer);
    return systemDatabase.recordWorkflowError(workflowId, serialized.serializedValue(), ownerXid);
  }

  /**
   * Record an ERROR result for a workflow that was never started, so that handles awaiting it fail
   * fast instead of polling forever. Used by internal workflows (e.g. the debouncer) that take
   * responsibility for starting a user workflow and must surface their own failures to the caller's
   * handle when they cannot.
   */
  public void recordErrorForUnstartedWorkflow(
      String workflowId,
      String workflowName,
      String className,
      @Nullable String instanceName,
      @Nullable Object[] args,
      Throwable error) {
    // No configured serializer means the built-in one, which only SerializationUtil knows:
    // naming it here dereferences null on the default configuration.
    var serializedArgs =
        SerializationUtil.serializeArgs(
            Objects.requireNonNullElseGet(args, () -> new Object[0]), null, null, this.serializer);
    String serialization = serializedArgs.serialization();
    var serializedError = SerializationUtil.serializeError(error, serialization, this.serializer);
    var initStatus =
        new WorkflowStatusInternal(
            workflowId,
            workflowName,
            className,
            instanceName,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            serializedArgs.serializedValue(),
            executorId(),
            appVersion(),
            appId(),
            null,
            null,
            null,
            serializedArgs.serialization(),
            null,
            null,
            null,
            false,
            null);
    systemDatabase.recordErrorForUnstartedWorkflow(initStatus, serializedError.serializedValue());
  }
}
