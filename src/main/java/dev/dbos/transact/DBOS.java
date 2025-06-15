package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.interceptor.TransactInvocationHandler;
import dev.dbos.transact.migration.DatabaseMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class DBOS {

    static Logger logger = LoggerFactory.getLogger(DBOS.class) ;

    public static <T> WorkflowBuilder<T> Workflow() {
        return new WorkflowBuilder<>();
    }

    // Optional static inner builder class for workflows
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
                    implementation
            );
        }
    }

    public static void launch(DBOSConfig config) {
        logger.info("Starting DBOS ...") ;
        DatabaseMigrator.runMigrations(config);
    }

    public static void launchAndWait(DBOSConfig config) {
        launch(config);
        logger.info("DBOS is now running. Press Ctrl+C to exit.");
        waitUntilShutdown(); // Block
    }

    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private static void waitUntilShutdown() {
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

