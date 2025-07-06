package dev.dbos.transact.exceptions;


/**
 * Wrapper to be thrown back to user from calls like getResult
 * It would be unsafe, error-prone and hard to try to throw the
 * actual user defined exception
 *
 */
public class DBOSAppException extends RuntimeException {
    public final SerializableException original;

    public DBOSAppException(String msg, SerializableException original) {
        super(msg);
        this.original = original;
    }
}
