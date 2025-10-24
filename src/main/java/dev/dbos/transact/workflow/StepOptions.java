package dev.dbos.transact.workflow;

/**
 * Step name and retry options. A step is retried if `retriesAllowed` is true, and maxAttempts is
 * greater than 1. Retries start `intervalSeconds` apart, and increase by a factor of `backOffRate`
 * with successive attempts.
 */
public record StepOptions(
    String name,
    boolean retriesAllowed,
    int maxAttempts,
    double intervalSeconds,
    double backOffRate) {
  public static final double DEFAULT_INTERVAL_SECONDS = 1.0;
  public static final double DEFAULT_BACKOFF = 2.0;

  /** Construct a StepOptions with specified name */
  public StepOptions(String name) {
    this(name, false, 1, StepOptions.DEFAULT_INTERVAL_SECONDS, StepOptions.DEFAULT_BACKOFF);
  }

  /** Construct a StepOptions with specified name and maximum retry attempts */
  public StepOptions(String name, int maxAttempts) {
    this(
        name,
        maxAttempts > 1,
        maxAttempts > 1 ? maxAttempts : 1,
        StepOptions.DEFAULT_INTERVAL_SECONDS,
        StepOptions.DEFAULT_BACKOFF);
  }

  /**
   * Construct a StepOptions with specified name and maximum retry attempts and initial retry
   * interval
   */
  public StepOptions(String name, int maxAttempts, double intervalSeconds) {
    this(
        name,
        maxAttempts > 1,
        maxAttempts > 1 ? maxAttempts : 1,
        intervalSeconds,
        StepOptions.DEFAULT_BACKOFF);
  }

  public StepOptions(
      String name,
      boolean retriesAllowed,
      int maxAttempts,
      double intervalSeconds,
      double backOffRate) {
    this.name = name;
    this.retriesAllowed = retriesAllowed;
    this.maxAttempts = maxAttempts;
    this.intervalSeconds = intervalSeconds;
    this.backOffRate = backOffRate;
  }

  /** Produces a new StepOptions with retry enabled/disabled. */
  public StepOptions withRetriesAllowed(boolean b) {
    return new StepOptions(this.name, b, this.maxAttempts, this.intervalSeconds, this.backOffRate);
  }

  /**
   * Produces a new StepOptions with the specified maximum attempts. Retry will be enabled if this
   * `n` is more than 1
   */
  public StepOptions withMaxAttempts(int n) {
    return new StepOptions(this.name, n > 1, n, this.intervalSeconds, this.backOffRate);
  }

  /** Produces a new StepOptions with the specified retry interval. */
  public StepOptions withIntervalSeconds(double t) {
    return new StepOptions(this.name, this.retriesAllowed, this.maxAttempts, t, this.backOffRate);
  }

  /** Produces a new StepOptions with the specified retry backoff rate. */
  public StepOptions withBackoffRate(double t) {
    return new StepOptions(
        this.name, this.retriesAllowed, this.maxAttempts, this.intervalSeconds, t);
  }

  /** Produces a new StepOptions with retries disabled. */
  public StepOptions noRetries() {
    return withRetriesAllowed(false).withMaxAttempts(1);
  }
}
