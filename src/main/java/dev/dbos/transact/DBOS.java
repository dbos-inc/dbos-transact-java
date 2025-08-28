package dev.dbos.transact;

import dev.dbos.transact.conductor.Conductor;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.database.GetWorkflowEventContext;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.RecoveryService;
import dev.dbos.transact.execution.WorkflowFunction;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;
import dev.dbos.transact.http.HttpServer;
import dev.dbos.transact.http.controllers.AdminController;
import dev.dbos.transact.interceptor.AsyncInvocationHandler;
import dev.dbos.transact.interceptor.QueueInvocationHandler;
import dev.dbos.transact.interceptor.UnifiedInvocationHandler;
import dev.dbos.transact.internal.QueueRegistry;
import dev.dbos.transact.internal.WorkflowRegistry;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.queue.ListQueuedWorkflowsInput;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.queue.QueueService;
import dev.dbos.transact.queue.RateLimit;
import dev.dbos.transact.scheduled.SchedulerService;
import dev.dbos.transact.tempworkflows.InternalWorkflowsService;
import dev.dbos.transact.tempworkflows.InternalWorkflowsServiceImpl;
import dev.dbos.transact.workflow.*;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOS {

    static Logger logger = LoggerFactory.getLogger(DBOS.class);

    private final WorkflowRegistry workflowRegistry = new WorkflowRegistry();
    private final QueueRegistry queueRegistry = new QueueRegistry();



    private final DBOSConfig config;
    private final SystemDatabase systemDatabase;
    private final DBOSExecutor dbosExecutor;
    private final QueueService queueService;
    private final SchedulerService schedulerService;
    private RecoveryService recoveryService;
    private HttpServer httpServer;
    private Conductor conductor;

    private InternalWorkflowsService internalWorkflowsService;
    private Queue internalQueue;
    private Queue schedulerQueue;

    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    private DBOS(DBOSConfig config) {
        this.config = config;
        var workflowMap = workflowRegistry.getSnapshot();
        systemDatabase = new SystemDatabase(config);
        dbosExecutor = new DBOSExecutor(config, workflowMap, systemDatabase);
        queueService = new QueueService(queueRegistry.getSnapshot(), systemDatabase, dbosExecutor);
        schedulerService = new SchedulerService(workflowMap, dbosExecutor);

        DBOSContextHolder.clear();
    }

    /**
     * Initializes the singleton instance of DBOS with config. Should be called once
     * during app startup. @DBOSConfig config dbos configuration
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
    SystemDatabase getSystemDatabase() {
        return systemDatabase;
    }

    DBOSExecutor getDbosExecutor() {
        return dbosExecutor;
    }

    QueueService getQueueService() {
        return queueService;
    }

    SchedulerService getSchedulerService() {
        return schedulerService;
    }

    void clearRegistry() {
        workflowRegistry.clear();
        queueRegistry.clear();
    }


    public void registerWorkflow(String workflowName, Object target, String targetClassName, Method method) {
        workflowRegistry.register(workflowName, target, targetClassName, method);
    }

    public WorkflowFunctionWrapper getWorkflow(String workflowName) {
        return workflowRegistry.get(workflowName);
    }

    public void register(Queue queue) {
        queueRegistry.register(queue);
    }

    public <T> WorkflowBuilder<T> Workflow() {
        return new WorkflowBuilder<>(this);
    }

    public QueueBuilder Queue(String name) {
        return new QueueBuilder(this, name);
    }

    // inner builder class for workflows
    public static class WorkflowBuilder<T> {
        private final DBOS dbos;
        private Class<T> interfaceClass;
        private Object implementation;
        private boolean async;
        private Queue queue;

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

            dbos.registerWorkflow(interfaceClass, implementation);

            if (async) {
                return AsyncInvocationHandler.createProxy(interfaceClass,
                        implementation,
                        dbos.dbosExecutor);
            } else if (queue != null) {
                return QueueInvocationHandler.createProxy(interfaceClass,
                        implementation,
                        queue,
                        dbos.dbosExecutor);
            } else {
                return UnifiedInvocationHandler.createProxy(interfaceClass,
                        implementation,
                        dbos.dbosExecutor);
            }
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
         * @param name
         *            The name of the queue.
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
            dbos.queueRegistry.register(queue);
            return queue;
        }
    }

    private void registerWorkflow(Class<?> interfaceClass, Object implementation) {
        Objects.nonNull(interfaceClass);
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("interfaceClass must be an interface");
        }
        Objects.nonNull(implementation);

        Method[] methods = implementation.getClass().getDeclaredMethods();
        for (Method method : methods) {
            Workflow wfAnnotation = method.getAnnotation(Workflow.class);
            if (wfAnnotation != null) {
                String workflowName = wfAnnotation.name().isEmpty()
                        ? method.getName()
                        : wfAnnotation.name();
                method.setAccessible(true); // In case it's not public

                registerWorkflow(workflowName, implementation, implementation.getClass().getName(), method);
            }
        }

    }


    private void registerInternals() {
        internalWorkflowsService = this.<InternalWorkflowsService>Workflow()
                .interfaceClass(InternalWorkflowsService.class)
                .implementation(new InternalWorkflowsServiceImpl(this))
                .build();

        internalQueue = this.Queue(Constants.DBOS_INTERNAL_QUEUE).build();
        this.queueService.setInternalQueue(internalQueue);
        schedulerQueue = this.Queue("schedulerQueue").build();
        this.schedulerService.setSchedulerQueue(schedulerQueue);
    }

    public void launch() {
        dbosExecutor.start(this);

        logger.info("Executor ID: {}", dbosExecutor.getExecutorId());
        logger.info("Application version: {}", dbosExecutor.getAppVersion());

        queueService.start();

        schedulerService.start();

        String conductorKey = config.getConductorKey();
        if (conductorKey != null) {
            Conductor.Builder builder = new Conductor.Builder(systemDatabase, dbosExecutor, conductorKey);
            String domain = config.getConductorDomain();
            if (domain != null && !domain.trim().isEmpty()) {
                builder.domain(domain);
            }
            conductor = builder.build();
            conductor.start();
        }

        if (config.isHttp()) {
            httpServer = HttpServer.getInstance(config.getHttpPort(),
                    new AdminController(systemDatabase, dbosExecutor, queueRegistry.getSnapshot()));
            if (config.isHttpAwaitOnStart()) {
                Thread httpThread = new Thread(() -> {
                    logger.info("Start http in background thread");
                    httpServer.startAndBlock();
                }, "http-server-thread");
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
        logger.debug("shutdown() called");

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

            if (conductor != null) {
                conductor.stop();
            }

            if (config.isHttp()) {
                httpServer.stop();
            }

            // TODO: https://github.com/dbos-inc/dbos-transact-java/issues/51
            // systemDatabase.destroy();
        }
    }

    public <T> WorkflowHandle<T> retrieveWorkflow(String workflowId) {
        return this.dbosExecutor.retrieveWorkflow(workflowId);
    }

    /**
     * Scans the class for all methods that have Workflow and Scheduled annotations
     * and schedules them for execution
     *
     * @param implementation
     *            instance of a class
     */
    public void scheduleWorkflow(Object implementation) {
        schedulerService.scanAndSchedule(implementation);
    }

    /**
     * Send a message to a workflow
     *
     * @param destinationId
     *            recipient of the message
     * @param message
     *            message to be sent
     * @param topic
     *            topic to which the message is send
     */
    public void send(String destinationId, Object message, String topic) {
        DBOSContext ctx = DBOSContextHolder.get();
        if (!ctx.isInWorkflow()) {
            this.internalWorkflowsService.sendWorkflow(destinationId, message, topic);
            return;
        }
        int stepFunctionId = ctx.getAndIncrementFunctionId();

        systemDatabase.send(ctx.getWorkflowId(), stepFunctionId, destinationId, message, topic);
    }

    /**
     * Get a message sent to a particular topic
     *
     * @param topic
     *            the topic whose message to get
     * @param timeoutSeconds
     *            time in seconds after which the call times out
     * @return the message if there is one or else null
     */
    public Object recv(String topic, float timeoutSeconds) {
        DBOSContext ctx = DBOSContextHolder.get();
        if (!ctx.isInWorkflow()) {
            throw new IllegalArgumentException("recv() must be called from a workflow.");
        }
        int stepFunctionId = ctx.getAndIncrementFunctionId();
        int timeoutFunctionId = ctx.getAndIncrementFunctionId();

        return systemDatabase.recv(ctx.getWorkflowId(),
                stepFunctionId,
                timeoutFunctionId,
                topic,
                timeoutSeconds);
    }

    /**
     * Call within a workflow to publish a key value pair
     *
     * @param key
     *            identifier for published data
     * @param value
     *            data that is published
     */
    public void setEvent(String key, Object value) {
        logger.info("Received setEvent for key " + key);

        DBOSContext ctx = DBOSContextHolder.get();
        if (!ctx.isInWorkflow()) {
            throw new IllegalArgumentException("send must be called from a workflow.");
        }
        int stepFunctionId = ctx.getAndIncrementFunctionId();

        systemDatabase.setEvent(ctx.getWorkflowId(), stepFunctionId, key, value);
    }

    /**
     * Get the data published by a workflow
     *
     * @param workflowId
     *            id of the workflow who data is to be retrieved
     * @param key
     *            identifies the data
     * @param timeOut
     *            time in seconds to wait for data
     * @return the published value or null
     */
    public Object getEvent(String workflowId, String key, float timeOut) {
        logger.info("Received getEvent for " + workflowId + " " + key);

        DBOSContext ctx = DBOSContextHolder.get();

        if (ctx.isInWorkflow()) {
            int stepFunctionId = ctx.getAndIncrementFunctionId();
            int timeoutFunctionId = ctx.getAndIncrementFunctionId();
            GetWorkflowEventContext callerCtx = new GetWorkflowEventContext(ctx.getWorkflowId(),
                    stepFunctionId, timeoutFunctionId);
            return systemDatabase.getEvent(workflowId, key, timeOut, callerCtx);
        }

        return systemDatabase.getEvent(workflowId, key, timeOut, null);
    }

    /**
     * Durable sleep. When you are in a workflow, use this instead of Thread.sleep.
     * On restart or during recovery the original expected wakeup time is honoured
     * as opposed to sleeping all over again.
     *
     * @param seconds
     *            in seconds
     */
    public void sleep(float seconds) {

        this.dbosExecutor.sleep(seconds);
    }

    /**
     * Resume a workflow starting from the step after the last complete step
     *
     * @param workflowId
     *            id of the workflow
     * @return A handle to the workflow
     */
    public <T> WorkflowHandle<T> resumeWorkflow(String workflowId) {
        return this.dbosExecutor.resumeWorkflow(workflowId);
    }

    /***
     *
     * Cancel the workflow. After this function is called, the next step (not the
     * current one) will not execute
     *
     * @param workflowId
     */

    public void cancelWorkflow(String workflowId) {
        this.dbosExecutor.cancelWorkflow(workflowId);
    }

    /**
     * Fork the workflow. Re-execute with another Id from the step provided. Steps
     * prior to the provided step are copied over
     *
     * @param workflowId
     *            Original workflow Id
     * @param startStep
     *            Start execution from this step. Prior steps copied over
     * @param options
     *            {@link ForkOptions} containing forkedWorkflowId,
     *            applicationVersion, timeout
     * @return handle to the workflow
     */
    public <T> WorkflowHandle<T> forkWorkflow(String workflowId, int startStep,
            ForkOptions options) {
        return this.dbosExecutor.forkWorkflow(workflowId, startStep, options);
    }

    /**
     * Start a workflow asynchronously. If a queue is specified with DBOSOptions,
     * the workflow is queued.
     *
     * @param func
     *            A function annotated with @Workflow
     * @return handle {@link WorkflowHandle} to the workflow
     * @param <T>
     *            type returned by the function
     */
    public <T> WorkflowHandle<T> startWorkflow(WorkflowFunction<T> func) {
        return this.dbosExecutor.startWorkflow(func);
    }

    /**
     * List all workflows
     *
     * @param input
     *            {@link ListWorkflowsInput} parameters to query workflows
     * @return a list of workflow status {@link WorkflowStatus}
     */
    public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
        return systemDatabase.listWorkflows(input);
    }

    /**
     * List the steps in the workflow
     *
     * @param workflowId
     *            Id of the workflow whose steps to return
     * @return list of step information {@link StepInfo}
     */
    public List<StepInfo> listWorkflowSteps(String workflowId) {
        return systemDatabase.listWorkflowSteps(workflowId);
    }

    /**
     * List workflows queued
     *
     * @param query
     *            parameters to query by
     * @param loadInput
     *            Whether to load input or not
     * @return list of workflow statuses {@link WorkflowStatus}
     */
    public List<WorkflowStatus> listQueuedWorkflows(ListQueuedWorkflowsInput query, boolean loadInput) {
        return systemDatabase.getQueuedWorkflows(query, loadInput);
    }
}
