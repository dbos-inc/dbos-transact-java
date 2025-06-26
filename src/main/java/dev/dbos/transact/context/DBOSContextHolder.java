package dev.dbos.transact.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSContextHolder {
    private static final ThreadLocal<DBOSContext> contextHolder = ThreadLocal.withInitial(DBOSContext::new);
    private static Logger logger = LoggerFactory.getLogger(DBOSContextHolder.class);

    public static DBOSContext get() {
        return contextHolder.get();
    }

    public static void clear() {
        contextHolder.remove();
        logger.debug("context cleared for thread " + Thread.currentThread().getId());
    }

    public static void set(DBOSContext context) {
        contextHolder.set(context);
        logger.debug("context set for thread " + Thread.currentThread().getId());
    }

}

