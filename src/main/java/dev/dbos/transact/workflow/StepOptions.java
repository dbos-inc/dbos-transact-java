package dev.dbos.transact.workflow;

public record StepOptions(
    String name,
    boolean retriesAllowed,
    int maxAttempts,
    double intervalSeconds,
    double backOffRate) {
  public static final double DEFAULT_INTERVAL_SECONDS = 1.0;
  public static final double DEFAULT_BACKOFF = 2.0;

  // --- Ctors - Overloads to simulate "default args"
  public StepOptions(String name) {
    this(name, false, 1, StepOptions.DEFAULT_INTERVAL_SECONDS, StepOptions.DEFAULT_BACKOFF);
  }

  public StepOptions(String name, int maxAttempts) {
    this(
        name,
        maxAttempts > 1,
        maxAttempts > 1 ? maxAttempts : 1,
        StepOptions.DEFAULT_INTERVAL_SECONDS,
        StepOptions.DEFAULT_BACKOFF);
  }

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

  // --- Static factories (nice at call sites)
  public static StepOptions of(String name) {
    return new StepOptions(name);
  }

  // --- Withers for chaining (immutability preserved)
  public StepOptions withRetriesAllowed(boolean b) {
    return new StepOptions(this.name, b, this.maxAttempts, this.intervalSeconds, this.backOffRate);
  }

  public StepOptions withMaxAttempts(int n) {
    return new StepOptions(this.name, n > 1, n, this.intervalSeconds, this.backOffRate);
  }

  public StepOptions withIntervalSeconds(double t) {
    return new StepOptions(this.name, this.retriesAllowed, this.maxAttempts, t, this.backOffRate);
  }

  public StepOptions withBackoffRate(double t) {
    return new StepOptions(
        this.name, this.retriesAllowed, this.maxAttempts, this.intervalSeconds, t);
  }

  // Optional sugar for very common tweaks
  public StepOptions noRetries() {
    return withRetriesAllowed(false).withMaxAttempts(1);
  }
}
