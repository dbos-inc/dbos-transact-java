package dev.dbos.transact.internal;

import dev.dbos.transact.context.DBOSContext;

public class DBOSContextHolder {
    private static final ThreadLocal<DBOSContext> contextHolder = ThreadLocal.withInitial(DBOSContext::new);

    public static DBOSContext get() {
        return contextHolder.get();
    }

    public static void clear() {
        contextHolder.remove();
    }

    public static void set(DBOSContext context) {
        contextHolder.set(context);
    }
}
