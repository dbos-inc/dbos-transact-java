package dev.dbos.transact.exceptions;

public class DBOSAppException extends RuntimeException {
    public final SerializableException original;

    public DBOSAppException(String msg, SerializableException original) {
        super(msg);
        this.original = original;
    }
}
