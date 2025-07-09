package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.interceptor.AsyncInvocationHandler;
import dev.dbos.transact.interceptor.QueueInvocationHandler;
import dev.dbos.transact.interceptor.TransactInvocationHandler;
import dev.dbos.transact.migrations.DatabaseMigrator;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.queue.QueueService;
import dev.dbos.transact.queue.RateLimit;
import dev.dbos.transact.scheduled.SchedulerService;
import dev.dbos.transact.workflow.WorkflowHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class DBOS {

    static Logger logger = LoggerFactory.getLogger(DBOS.class) ;

    private static DBOS instance;

    private final DBOSConfig config;
    private DBOSExecutor dbosExecutor  ;
    private QueueService queueService ;
    private SchedulerService schedulerService ;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    private DBOS(DBOSConfig config) {
        this.config = config;
    }

    /**
     * Initializes the singleton instance of DBOS with config.
     * Should be called once during app startup.
     *
     * @DBOSConfig config dbos configuration
     */
    public static synchronized void initialize(DBOSConfig config) {
        if (instance != null) {
            throw new IllegalStateException("DBOS has already been initialized.");
        }
        instance = new DBOS(config);
    }

    /**
     * Gets the singleton instance of DBOS.
     * Throws if accessed before initialization.
     */
    public static DBOS getInstance() {
        if (instance == null) {
            throw new IllegalStateException("DBOS has not been initialized. Call DBOS.initialize() first.");
        }
        return instance;
    }

    public void setDbosExecutor(DBOSExecutor executor) {
        this.dbosExecutor = executor;
    }

    public void setQueueService(QueueService queueService) {
        this.queueService = queueService;
    }

    public void setSchedulerService(SchedulerService schedulerService) {
        this.schedulerService = schedulerService ;
    }

    public <T> WorkflowBuilder<T> Workflow() {
        return new WorkflowBuilder<>();
    }

    // inner builder class for workflows
    public static class WorkflowBuilder<T> {
        private Class<T> interfaceClass;
        private Object implementation;
        private boolean async ;
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
            this.async = true ;
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
                        interfaceClass,
                        implementation,
                        DBOS.getInstance().dbosExecutor
                );
            } else if(queue != null) {
                return QueueInvocationHandler.createProxy(
                        interfaceClass,
                        implementation,
                        queue,
                        DBOS.getInstance().dbosExecutor
                ) ;
            } else {
                return TransactInvocationHandler.createProxy(
                        interfaceClass,
                        implementation,
                        DBOS.getInstance().dbosExecutor
                );
            }

        }
    }

    public static class QueueBuilder {

        private final String name;

        private int concurrency ;
        private int workerConcurrency ;
        private RateLimit limit ;
        private boolean priorityEnabled = false;

        /**
         * Constructor for the Builder, taking the required 'name' field.
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
        DatabaseMigrator.runMigrations(config);
        if (dbosExecutor == null) {
            SystemDatabase.initialize(config);
            dbosExecutor = new DBOSExecutor(config, SystemDatabase.getInstance());
        }

      if (queueService == null) {
          logger.info("launch starting queue service");
          queueService = new QueueService(SystemDatabase.getInstance());
          queueService.setDbosExecutor(dbosExecutor);
          queueService.start();
      } else {
          queueService.start();
      }


        if (schedulerService == null) {
            schedulerService = new SchedulerService(dbosExecutor);
            schedulerService.start();
        } else {
            schedulerService.start();
        }

        // Block the main thread until shutdown is called
        Thread blocker = new Thread(() -> {
            try {
                shutdownLatch.await(); // Blocks until latch is counted down
            } catch (InterruptedException ignored) {
            }
        }, "DBOS-MainBlocker");

        blocker.setDaemon(false); // Prevents JVM from exiting
        blocker.start();

        // Hook for Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Ctrl+C received. Shutting down DBOS...");
            shutdown(); // Triggers latch and cleanup
        }));


    }

    public void shutdown() {

        if (isShutdown.compareAndSet(false, true)) {

            if (queueService != null) {
                queueService.stop();
                queueService = null;
            }
            if (dbosExecutor != null) {
                dbosExecutor.shutdown();
                dbosExecutor = null;
            }

            // schedulerService.stop();
            shutdownLatch.countDown();
            instance = null;
        }
    }

    public static WorkflowHandle retrieveWorkflow(String workflowId) {
        return DBOS.getInstance().dbosExecutor.retrieveWorkflow(workflowId);
    }

    /**
     * Scans the class for all methods that have Workflow and Scheduled annotations
     * and schedules them for execution
     *
     * @param implementation instance of a class
     */
    public void scheduleWorkflow(Object implementation) {
        schedulerService.scanAndSchedule(implementation);
    }

}

