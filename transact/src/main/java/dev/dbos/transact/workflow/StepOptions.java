package dev.dbos.transact.workflow;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

import org.jspecify.annotations.NonNull;

public record StepOptions(
    String name,
    int maxAttempts,
    Duration retryInterval,
    double backOffRate,
    Predicate<Throwable> shouldRetry) {

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

  public StepOptions(String name, int maxAttempts, Duration retryInterval, double backOffRate) {
    this(name, maxAttempts, retryInterval, backOffRate, null);
  }

  public StepOptions(String name) {
    this(
        name,
        1,
        Duration.ofSeconds((long) StepOptions.DEFAULT_INTERVAL_SECONDS),
        StepOptions.DEFAULT_BACKOFF,
        null);
  }

  public static StepOptions create(Step stepTag, Method method) {
    var name = stepTag.name().isEmpty() ? method.getName() : stepTag.name();
    var maxAttempts = stepTag.maxAttempts() < 1 ? 1 : stepTag.maxAttempts();
    var interval = Duration.ofMillis((long) (stepTag.intervalSeconds() * 1000));

    var shouldRetryClass = stepTag.shouldRetry();
    if (shouldRetryClass.equals(StepShouldRetry.None.class)) {
      return new StepOptions(name, maxAttempts, interval, stepTag.backOffRate());
    }

    try {
      var instance = shouldRetryClass.getConstructor().newInstance();
      return new StepOptions(
          name, maxAttempts, interval, stepTag.backOffRate(), instance::shouldRetry);
    } catch (NoSuchMethodException ex) {
      throw new RuntimeException(
          "%s must be a public class with a public no-arg constructor"
              .formatted(shouldRetryClass.getName()),
          ex);
    } catch (Exception ex) {
      throw new RuntimeException(
          "Failed to instantiate shouldRetry class: " + shouldRetryClass.getName(), ex);
    }
  }

  public StepOptions withMaxAttempts(int maxAttempts) {
    return new StepOptions(
        this.name, maxAttempts, this.retryInterval, this.backOffRate, this.shouldRetry);
  }

  public StepOptions withRetryInterval(Duration interval) {
    return new StepOptions(
        this.name, this.maxAttempts, interval, this.backOffRate, this.shouldRetry);
  }

  public StepOptions withBackoffRate(double backOffRate) {
    return new StepOptions(
        this.name, this.maxAttempts, this.retryInterval, backOffRate, this.shouldRetry);
  }

  public StepOptions withShouldRetry(Predicate<Throwable> shouldRetry) {
    return new StepOptions(
        this.name, this.maxAttempts, this.retryInterval, this.backOffRate, shouldRetry);
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
