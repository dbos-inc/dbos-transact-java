package dev.dbos.transact.database;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

/**
 * Represents a piece of state associated with an external service such as an event dispatcher.
 *
 * <p>This record models a key/value entry stored by the DBOS system database on behalf of an
 * external service It includes identifying information (service name, fully qualified workflow
 * name, key within), a value, and optional metadata for versioning the update.
 *
 * <p>The canonical constructor enforces that {@code service}, {@code workflowName}, and {@code key}
 * are non-null.
 *
 * @param service The name of the external service that owns or stores the state.
 * @param workflowName The fully qualified function name of the workflow that this state belongs to.
 * @param key The key under which the external state is stored, allowing multiple values per service
 *     and workflow combination.
 * @param value The current value associated with the key.
 * @param updateTime The timestamp of the last update, represented as a decimal (e.g., UNIX epoch
 *     seconds), or {@code null} if unused.
 * @param updateSeq A monotonic sequence number for updates, used to detect the latest version, or
 *     {@code null} if not applicable.
 */
public record ExternalState(
    String service,
    String workflowName,
    String key,
    String value,
    BigDecimal updateTime,
    BigInteger updateSeq) {

  public ExternalState {
    Objects.requireNonNull(service, "service must not be null");
    Objects.requireNonNull(workflowName, "workflowName must not be null");
    Objects.requireNonNull(key, "key must not be null");
  }

  /**
   * Create an external state
   *
   * @param service service storing the state
   * @param workflowName fully qualified name of the workflow storine for which the state applies
   * @param key key allowing multiple values to be stored per service per workflow
   */
  public ExternalState(String service, String workflowName, String key) {
    this(service, workflowName, key, null, null, null);
  }

  /**
   * Create an {@code ExternalState} like this one, but with a new value
   *
   * @param value
   */
  public ExternalState withValue(String value) {
    return new ExternalState(service, workflowName, key, value, updateTime, updateSeq);
  }

  /**
   * Create an {@code ExternalState} like this one, but with a new update time
   *
   * @param updateTime update time to use, as a decimal number of seconds since the Unix epoch
   */
  public ExternalState withUpdateTime(BigDecimal updateTime) {
    return new ExternalState(service, workflowName, key, value, updateTime, updateSeq);
  }

  /**
   * Create an {@code ExternalState} like this one, but with a new sequence number
   *
   * @param updateSeq sequence number to use
   */
  public ExternalState withUpdateSeq(BigInteger updateSeq) {
    return new ExternalState(service, workflowName, key, value, updateTime, updateSeq);
  }
}
