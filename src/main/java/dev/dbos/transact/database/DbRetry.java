package dev.dbos.transact.database;

import dev.dbos.transact.exceptions.DBOSSystemDatabaseException;
import dev.dbos.transact.execution.ThrowingRunnable;
import dev.dbos.transact.execution.ThrowingSupplier;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DbRetry {
  private static final Logger logger = LoggerFactory.getLogger(DbRetry.class);

  private DbRetry() {}

  public record Options(
      Duration initialBackoff,
      Duration maxBackoff,
      int maxAttempts,
      Predicate<Throwable> retriablePredicate) {
    public static Options defaults() {
      return new Options(
          Duration.ofSeconds(1), Duration.ofSeconds(60), 10, DbRetry::isRetriableSql);
    }

    public Options withInitialBackoff(Duration d) {
      return new Options(Objects.requireNonNull(d), maxBackoff, maxAttempts, retriablePredicate);
    }

    public Options withMaxBackoff(Duration d) {
      return new Options(
          initialBackoff, Objects.requireNonNull(d), maxAttempts, retriablePredicate);
    }

    public Options withMaxAttempts(int n) {
      return new Options(initialBackoff, maxBackoff, n, retriablePredicate);
    }

    public Options withRetriablePredicate(Predicate<Throwable> p) {
      return new Options(initialBackoff, maxBackoff, maxAttempts, Objects.requireNonNull(p));
    }
  }

  /** Call with defaults: backoff 1s..60s, maxAttempts=10, SQL-transient detection. */
  public static <T> T call(ThrowingSupplier<T, Exception> body) {
    return call(body, Options.defaults());
  }

  /** Call with custom Options. */
  public static <T, E extends Exception> T call(ThrowingSupplier<T, E> body, Options opts) {
    Objects.requireNonNull(body);
    Objects.requireNonNull(opts);

    int attempt = 0;
    Duration backoff = opts.initialBackoff;
    Throwable last = null;

    while (attempt < Math.max(1, opts.maxAttempts)) {
      try {
        return body.execute();
      } catch (Throwable t) {
        last = t;

        // Only retry if it's deemed retriable (transient)
        if (!opts.retriablePredicate.test(t)) {
          // Non-transient -> propagate unchecked
          throw wrapUnchecked(t);
        }

        attempt++;
        if (attempt >= opts.maxAttempts) break;

        // jitter: backoff * (0.5 .. 1.5)
        double jitterFactor = 0.5 + ThreadLocalRandom.current().nextDouble();
        long sleepMillis = Math.max(1L, (long) (backoff.toMillis() * jitterFactor));

        logger.warn(
            "DB operation failed (attempt {} of {}): {}. Retrying in {} ms",
            attempt,
            opts.maxAttempts,
            t.getMessage(),
            sleepMillis);

        sleepUninterruptibly(sleepMillis);

        // exponential backoff, capped
        long next = Math.min(backoff.toMillis() * 2, opts.maxBackoff.toMillis());
        backoff = Duration.ofMillis(next);
      }
    }

    String msg = "Database operation failed after %d attempts".formatted(opts.maxAttempts);
    throw new RuntimeException(msg, last);
  }

  /** Void variant. */
  public static <E extends Exception> void run(ThrowingRunnable<E> body) {
    call(
        () -> {
          body.execute();
          return null;
        });
  }

  public static <E extends Exception> void run(ThrowingRunnable<E> body, Options opts) {
    call(
        () -> {
          body.execute();
          return null;
        },
        opts);
  }

  // -------- Helpers --------

  private static RuntimeException wrapUnchecked(Throwable t) {
    return (t instanceof RuntimeException re) ? re : new DBOSSystemDatabaseException(t);
  }

  private static void sleepUninterruptibly(long millis) {
    boolean interrupted = false;
    long end = System.currentTimeMillis() + millis;
    while (true) {
      long now = System.currentTimeMillis();
      long remaining = end - now;
      if (remaining <= 0) break;
      try {
        Thread.sleep(remaining);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        // remember and keep sleeping to honor backoff; re-set later
        interrupted = true;
      }
    }
    if (interrupted) Thread.currentThread().interrupt();
  }

  /**
   * Default transient checker: - SQLTransientException (JDBC) and SQLRecoverableException -
   * SQLState prefixes commonly retriable: "08" connection exception "40" transaction rollback
   * (e.g., 40001 serialization, 40P01 deadlock in Postgres) Plus a few notable Postgres codes:
   * 55P03 lock_not_available, 53300 too_many_connections
   */
  private static boolean isRetriableSql(Throwable t) {
    // Walk cause chain to find an SQLException and check each along the way
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      if (cur instanceof SQLTransientException || cur instanceof SQLRecoverableException) {
        return true;
      }
      if (cur instanceof SQLException sqlEx) {
        String state = sqlEx.getSQLState();
        if (state == null) continue;

        if (state.startsWith("08")) return true; // connection exception
        if (state.startsWith("40")) return true; // transaction rollback (deadlock, serialization)
        if (state.equals("55P03")) return true; // lock_not_available (Postgres)
        if (state.equals("53300")) return true; // too_many_connections (Postgres)
        if (state.equals("57014")) return true; // query_canceled (often transient)
        if (state.equals("40001")) return true; // serialization_failure (explicit)
        if (state.equals("40P01")) return true; // deadlock_detected (explicit)
      }
    }
    return false;
  }
}
