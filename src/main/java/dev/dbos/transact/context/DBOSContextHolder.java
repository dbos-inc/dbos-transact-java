package dev.dbos.transact.context;

public class DBOSContextHolder {
    private static final ThreadLocal<DBOSContext> contextHolder = ThreadLocal.withInitial(DBOSContext::new);

    public static DBOSContext get() {
        return contextHolder.get();
    }

    public static void clear() {
        System.out.println("mjjjj context being cleared");
        contextHolder.remove();
    }

    public static void set(DBOSContext context) {
        contextHolder.set(context);
    }

}

