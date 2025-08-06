package dev.dbos.transact.exceptions;

/**
 * Wrapper for safe serialization of exceptions to the database. Serializing a user
 * defined Throwable as is can be huge.
 */
public class SerializableException {
    public final String className;
    public final String message;
    public final SerializableException cause;

    private static final int MAX_CAUSE_DEPTH = 5;

    public SerializableException(Throwable t) {
        this(t, 0);
    }

    private SerializableException(Throwable t, int depth) {
        this.className = t.getClass().getName();
        this.message = t.getMessage();
        if (t.getCause() != null && depth < MAX_CAUSE_DEPTH) {
            this.cause = new SerializableException(t.getCause(), depth + 1);
        }
        else {
            this.cause = null;
        }
    }

    // Optionally add no-arg constructor for Jackson
    public SerializableException() {
        this.className = null;
        this.message = null;
        this.cause = null;
    }
}
