package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.RecoveryService;
import dev.dbos.transact.execution.WorkflowFunction;
import dev.dbos.transact.http.HttpServer;
import dev.dbos.transact.http.controllers.AdminController;
import dev.dbos.transact.interceptor.AsyncInvocationHandler;
import dev.dbos.transact.interceptor.QueueInvocationHandler;
import dev.dbos.transact.interceptor.UnifiedInvocationHandler;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.notifications.NotificationService;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.queue.QueueService;
import dev.dbos.transact.queue.RateLimit;
import dev.dbos.transact.scheduled.SchedulerService;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.WorkflowHandle;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOS {

  static Logger logger = LoggerFactory.getLogger(DBOS.class);

  private static DBOS instance;

  private final DBOSConfig config;
  private final SystemDatabase systemDatabase;
  private final DBOSExecutor dbosExecutor;
  private final QueueService queueService;
  private final SchedulerService schedulerService;
  private NotificationService notificationService;
  private HttpServer httpServer;
  private RecoveryService recoveryService;

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private DBOS(DBOSConfig config) {
    this.config = config;
    systemDatabase = new SystemDatabase(config);
    dbosExecutor = new DBOSExecutor(config, systemDatabase);
    queueService = new QueueService(systemDatabase, dbosExecutor);
    queueService.setDbosExecutor(dbosExecutor);
    schedulerService = new SchedulerService(dbosExecutor);
  }

  private DBOS(
      DBOSConfig config, SystemDatabase sd, DBOSExecutor de, QueueService q, SchedulerService s) {
    this.config = config;
    this.systemDatabase = sd;
    this.dbosExecutor = de;
    this.queueService = q == null ? new QueueService(sd, dbosExecutor) : q;
    this.schedulerService = s == null ? new SchedulerService(de) : s;
  }

  /**
   * Initializes the singleton instance of DBOS with config. Should be called once during app
   * startup. @DBOSConfig config dbos configuration
   */
  public static synchronized DBOS initialize(DBOSConfig config) {
    if (instance != null) {
      throw new IllegalStateException("DBOS has already been initialized.");
    }
    if (config.migration()) {
      MigrationManager.runMigrations(config);
    }
    instance = new DBOS(config);
    return instance;
  }

  public static synchronized DBOS initialize(
      DBOSConfig config, SystemDatabase sd, DBOSExecutor de, QueueService q, SchedulerService ss) {
    if (instance != null) {
      throw new IllegalStateException("DBOS has already been initialized.");
    }

    if (config == null || sd == null || de == null) {
      throw new IllegalArgumentException("Config, systemdb, dbosexecutor cannot be null");
    }

    if (config.migration()) {
      MigrationManager.runMigrations(config);
    }
    instance = new DBOS(config, sd, de, q, ss);
    return instance;
  }

  /** Gets the singleton instance of DBOS. Throws if accessed before initialization. */
  public static DBOS getInstance() {
    if (instance == null) {
      throw new IllegalStateException(
          "DBOS has not been initialized. Call DBOS.initialize() first.");
    }
    return instance;
  }

  public <T> WorkflowBuilder<T> Workflow() {
    return new WorkflowBuilder<>();
  }

  // inner builder class for workflows
  public static class WorkflowBuilder<T> {
    private Class<T> interfaceClass;
    private Object implementation;
    private boolean async;
    private Queue queue;

    public WorkflowBuilder<T> interfaceClass(Class<T> iface) {
      this.interfaceClass = iface;
      return this;
    }

    public WorkflowBuilder<T> implementation(Object impl) {
      this.implementation = impl;
      return this;
    }

    public WorkflowBuilder<T> async() {
      this.async = true;
      return this;
    }

    public WorkflowBuilder<T> queue(Queue queue) {
      this.queue = queue;
      return this;
    }

    public T build() {
      if (interfaceClass == null || implementation == null) {
        throw new IllegalStateException("Interface and implementation must be set");
      }

      if (async) {
        return AsyncInvocationHandler.createProxy(
            interfaceClass, implementation, DBOS.getInstance().dbosExecutor);
      } else if (queue != null) {
        return QueueInvocationHandler.createProxy(
            interfaceClass, implementation, queue, DBOS.getInstance().dbosExecutor);
      } else {
        return UnifiedInvocationHandler.createProxy(
            interfaceClass, implementation, DBOS.getInstance().dbosExecutor);
      }
    }
  }

  public static class QueueBuilder {

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
    public QueueBuilder(String name) {
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

      Queue q = Queue.createQueue(name, concurrency, workerConcurrency, limit, priorityEnabled);
      DBOS.getInstance().queueService.register(q);
      return q;
    }
  }

  public void launch() {

    queueService.start();

    schedulerService.start();

    if (notificationService == null) {
      notificationService = systemDatabase.getNotificationService();
      notificationService.start();
    } else {
      notificationService.start();
    }

    if (config.isHttp()) {
      httpServer =
          HttpServer.getInstance(
              config.getHttpPort(), new AdminController(systemDatabase, dbosExecutor));
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

    recoveryService = new RecoveryService(dbosExecutor, systemDatabase);
    recoveryService.start();
  }

  public void shutdown() {

    if (isShutdown.compareAndSet(false, true)) {

      recoveryService.stop();

      if (queueService != null) {
        queueService.stop();
      }
      if (dbosExecutor != null) {
        dbosExecutor.shutdown();
      }

      if (schedulerService != null) {
        schedulerService.stop();
      }

      if (notificationService != null) {
        notificationService.stop();
      }

      if (config.isHttp()) {
        httpServer.stop();
      }

      systemDatabase.destroy();

      instance = null;
    }
  }

  public static <T> WorkflowHandle<T> retrieveWorkflow(String workflowId) {
    return DBOS.getInstance().dbosExecutor.retrieveWorkflow(workflowId);
  }

  /**
   * Scans the class for all methods that have Workflow and Scheduled annotations and schedules them
   * for execution
   *
   * @param implementation instance of a class
   */
  public void scheduleWorkflow(Object implementation) {
    schedulerService.scanAndSchedule(implementation);
  }

  /**
   * Send a message to a workflow
   *
   * @param destinationId recipient of the message
   * @param message message to be sent
   * @param topic topic to which the message is send
   */
  public void send(String destinationId, Object message, String topic) {
    notificationService.send(destinationId, message, topic);
  }

  /**
   * Get a message sent to a particular topic
   *
   * @param topic the topic whose message to get
   * @param timeoutSeconds time in seconds after which the call times out
   * @return the message if there is one or else null
   */
  public Object recv(String topic, float timeoutSeconds) {
    return notificationService.recv(topic, timeoutSeconds);
  }

  /**
   * Call within a workflow to publish a key value pair
   *
   * @param key identifier for published data
   * @param value data that is published
   */
  public void setEvent(String key, Object value) {
    notificationService.setEvent(key, value);
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
    return notificationService.getEvent(workflowId, key, timeOut);
  }

  /**
   * Durable sleep. When you are in a workflow, use this instead of Thread.sleep. On restart or
   * during recovery the original expected wakeup time is honoured as opposed to sleeping all over
   * again.
   *
   * @param seconds in seconds
   */
  public void sleep(float seconds) {

    this.dbosExecutor.sleep(seconds);
  }

  /**
   * Resume a workflow starting from the step after the last complete step
   *
   * @param workflowId id of the workflow
   * @return A handle to the workflow
   */
  public <T> WorkflowHandle<T> resumeWorkflow(String workflowId) {
    return this.dbosExecutor.resumeWorkflow(workflowId);
  }

  /***
   *
   * Cancel the workflow. After this function is called, the next step (not the current one)
   * will not execute
   *
   * @param workflowId
   */

  public void cancelWorkflow(String workflowId) {
    this.dbosExecutor.cancelWorkflow(workflowId);
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
  public <T> WorkflowHandle<T> forkWorkflow(String workflowId, int startStep, ForkOptions options) {
    return this.dbosExecutor.forkWorkflow(workflowId, startStep, options);
  }

  /**
   * Start a workflow asynchronously. If a queue is specified with DBOSOptions, the workflow is
   * queued.
   *
   * @param func A function annotated with @Workflow
   * @return handle {@link WorkflowHandle} to the workflow
   * @param <T> type returned by the function
   */
  public <T> WorkflowHandle<T> startWorkflow(WorkflowFunction<T> func) {
    return this.dbosExecutor.startWorkflow(func);
  }
}
