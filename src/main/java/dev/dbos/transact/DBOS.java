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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class DBOS {

    static Logger logger = LoggerFactory.getLogger(DBOS.class) ;

    private static DBOS instance;

    private final DBOSConfig config;
    private DBOSExecutor dbosExecutor  ;
    private QueueService queueService ;

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

        /**
         * Builds and returns a new WorkflowQueue instance.
         * The constructor's validation logic is executed here.
         * @return A new WorkflowQueue instance.
         * @throws IllegalArgumentException if validation rules are violated.
         */
        public Queue build() {

            // check queue is not registered

            Queue q = Queue.createQueue(name, concurrency, workerConcurrency, limit, priorityEnabled);
            DBOS.getInstance().queueService.register(q);
            return q;
        }
    }

    public void launch() {
        logger.info("Starting DBOS ...") ;
        DatabaseMigrator.runMigrations(config);
        if (dbosExecutor == null) {
            SystemDatabase.initialize(config);
            dbosExecutor = new DBOSExecutor(config, SystemDatabase.getInstance());
        }
        if (queueService == null) {
            queueService = new QueueService(SystemDatabase.getInstance());
            queueService.setDbosExecutor(dbosExecutor);
        }
        queueService.start();
    }

    public void shutdown() {
        queueService.stop();
        dbosExecutor.shutdown();
        instance = null ;
    }


    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public void waitUntilShutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received. Cleaning up...");
            shutdownLatch.countDown(); // Allow the main thread to exit
        }));

        try {
            shutdownLatch.await(); // Block main thread
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}

