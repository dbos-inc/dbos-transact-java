package dev.dbos.transact.workflow;

public record StepOptions(
    String name, boolean retriesAllowed, int maxAttempts, double backOffRate) {
  public static double DEFAULT_BACKOFF = 1.0;

  // --- Ctors - Overloads to simulate "default args"
  public StepOptions(String name) {
    this(name, false, 1, StepOptions.DEFAULT_BACKOFF);
  }

  public StepOptions(String name, int maxAttempts) {
    this(name, maxAttempts > 1, maxAttempts > 1 ? maxAttempts : 1, 1);
  }

  public StepOptions(String name, int maxAttempts, double backOffRate) {
    this(name, maxAttempts > 1, maxAttempts > 1 ? maxAttempts : 1, backOffRate);
  }

  public StepOptions(String name, boolean retriesAllowed, int maxAttempts, double backOffRate) {
    this.name = name;
    this.retriesAllowed = retriesAllowed;
    this.maxAttempts = maxAttempts;
    this.backOffRate = backOffRate;
  }

  // --- Static factories (nice at call sites)
  public static StepOptions of(String name) {
    return new StepOptions(name);
  }

  // --- Withers for chaining (immutability preserved)
  public StepOptions withRetriesAllowed(boolean b) {
    return new StepOptions(this.name, b, this.maxAttempts, this.backOffRate);
  }

  public StepOptions withMaxAttempts(int n) {
    return new StepOptions(this.name, n, this.backOffRate);
  }

  public StepOptions withBackoffRate(float t) {
    return new StepOptions(this.name, this.retriesAllowed, this.maxAttempts, t);
  }

  // Optional sugar for very common tweaks
  public StepOptions noRetries() {
    return withRetriesAllowed(false).withMaxAttempts(1);
  }
}
