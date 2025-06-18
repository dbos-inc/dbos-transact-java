package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.interceptor.TransactInvocationHandler;
import dev.dbos.transact.migration.DatabaseMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class DBOS {

    static Logger logger = LoggerFactory.getLogger(DBOS.class) ;

    private static DBOS instance;

    private final DBOSConfig config;
    private DBOSExecutor dbosExecutor  ;

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

    public <T> WorkflowBuilder<T> Workflow() {
        return new WorkflowBuilder<>();
    }

    // inner builder class for workflows
    public static class WorkflowBuilder<T> {
        private Class<T> interfaceClass;
        private T implementation;


        public WorkflowBuilder<T> interfaceClass(Class<T> iface) {
            this.interfaceClass = iface;
            return this;
        }

        public WorkflowBuilder<T> implementation(T impl) {
            this.implementation = impl;
            return this;
        }


        public T build() {
            if (interfaceClass == null || implementation == null) {
                throw new IllegalStateException("Interface and implementation must be set");
            }

            return TransactInvocationHandler.createProxy(
                    interfaceClass,
                    implementation,
                    DBOS.getInstance().dbosExecutor
            );
        }
    }

    public void launch() {
        logger.info("Starting DBOS ...") ;
        DatabaseMigrator.runMigrations(config);
        SystemDatabase.initialize(config);
        dbosExecutor = new DBOSExecutor(config, SystemDatabase.getInstance()) ;
    }

    public void launchAndWait() {
        launch();
        logger.info("DBOS is now running. Press Ctrl+C to exit.");
        waitUntilShutdown();
    }

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private void waitUntilShutdown() {
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

