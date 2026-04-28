package dev.dbos.transact.workflow;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Objects;

import org.jspecify.annotations.NonNull;

public record StepOptions(
    String name, int maxAttempts, Duration retryInterval, double backOffRate) {

  public static final double DEFAULT_INTERVAL_SECONDS = 1.0;
  public static final double DEFAULT_BACKOFF = 2.0;

  public StepOptions {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be greater than or equal to one");
    }
    if (retryInterval.isNegative() || retryInterval.isZero()) {
      throw new IllegalArgumentException("retryInterval must be positive");
    }
    if (backOffRate < 1.0) {
      throw new IllegalArgumentException("backOffRate must be greater than or equal to 1.0");
    }
  }

  public StepOptions(String name) {
    this(
        name,
        1,
        Duration.ofSeconds((long) StepOptions.DEFAULT_INTERVAL_SECONDS),
        StepOptions.DEFAULT_BACKOFF);
  }

  public static StepOptions create(Step stepTag, Method method) {
    var name = stepTag.name().isEmpty() ? method.getName() : stepTag.name();
    var maxAttempts = stepTag.maxAttempts() < 1 ? 1 : stepTag.maxAttempts();
    var interval = Duration.ofMillis((long) (stepTag.intervalSeconds() * 1000));
    return new StepOptions(name, maxAttempts, interval, stepTag.backOffRate());
  }

  public StepOptions withMaxAttempts(int maxAttempts) {
    return new StepOptions(this.name, maxAttempts, this.retryInterval, this.backOffRate);
  }

  public StepOptions withRetryInterval(Duration interval) {
    return new StepOptions(this.name, this.maxAttempts, interval, this.backOffRate);
  }

  public StepOptions withBackoffRate(double backOffRate) {
    return new StepOptions(this.name, this.maxAttempts, this.retryInterval, backOffRate);
  }

  @Override
  public @NonNull String name() {
    return Objects.requireNonNullElse(name, "");
  }

  @Override
  public int maxAttempts() {
    return maxAttempts < 1 ? 1 : maxAttempts;
  }

  @Override
  public Duration retryInterval() {
    return Objects.requireNonNullElse(retryInterval, Duration.ZERO);
  }
}
