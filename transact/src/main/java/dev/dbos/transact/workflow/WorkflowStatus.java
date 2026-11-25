package dev.dbos.transact.workflow;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.annotation.JsonSerialize;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.ser.std.StdScalarSerializer;

/**
 * Represents the status and metadata of a workflow execution. Contains information such as workflow
 * identifiers, state, timing, user context, and execution details.
 */
public record WorkflowStatus(
    /** Unique identifier for the workflow instance. */
    String workflowId,
    /** Current state of the workflow. JSON writes {@link #statusValue} in its place. */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) WorkflowState status,
    /** Name of the workflow. */
    String workflowName,
    /** Class name of the workflow implementation. */
    String className,
    /** Instance name of the workflow. */
    String instanceName,
    /** Authenticated user who initiated the workflow. */
    String authenticatedUser,
    /** Assumed role for the workflow execution. */
    String assumedRole,
    /** Roles authenticated for the workflow. */
    List<String> authenticatedRoles,
    /** Input arguments to the workflow. */
    Object[] input,
    /** Output/result of the workflow execution. */
    Object output,
    /** Error result if the workflow failed. */
    ErrorResult error,
    /** Identifier of the executor handling the workflow. */
    String executorId,
    /** When the workflow was created. */
    Instant createdAt,
    /** When the workflow was last updated. */
    Instant updatedAt,
    /** Application version. */
    String appVersion,
    /** Application identifier. */
    String appId,
    /** Number of recovery attempts made. */
    Integer recoveryAttempts,
    /** Name of the queue the workflow is assigned to. */
    String queueName,
    /** Timeout for the workflow execution. */
    Duration timeout,
    /** Deadline for the workflow execution. */
    Instant deadline,
    /** When the workflow started executing. */
    Instant startedAt,
    /** Deduplication identifier for the workflow. */
    String deduplicationId,
    /** Priority of the workflow in the queue. */
    Integer priority,
    /** Partition key for the queue. */
    String queuePartitionKey,
    /** Workflow ID from which this workflow was forked. */
    String forkedFrom,
    /** Parent workflow ID if this is a sub-workflow. */
    String parentWorkflowId,
    /** Whether another workflow was forked from this one. */
    Boolean wasForkedFrom,
    /** Time until which the workflow is delayed. */
    Instant delayUntil,
    /** When the workflow completed (terminal states only). */
    Instant completedAt,
    /** Serialized representation of the workflow. */
    String serialization,
    /** Custom JSON-serializable key-value attributes attached to the workflow at creation. */
    Map<String, Object> attributes,
    /** Name of the schedule that triggered this workflow, if any. */
    String scheduleName,
    /**
     * The application that owns this workflow, or null if it is unclaimed and so belongs to every
     * application sharing the system database.
     */
    String applicationName,
    /**
     * Whether this is a debounced workflow, whose deduplication ID is a debounce key cleared when
     * it leaves DELAYED; null when read from an export that predates the field.
     */
    Boolean isDebounced,
    /** The latest a debounced workflow's delay may be extended to, or null for no cap. */
    Instant debounceDeadline,
    /**
     * The status as stored, which tells what {@link WorkflowState#UNKNOWN} stands for: a status
     * this version does not know, such as one a newer version wrote. Null exactly when {@code
     * status} is.
     */
    String statusValue) {

  /**
   * Fills whichever of {@code status} and {@code statusValue} is missing in from the other: a known
   * {@code status} gives its name, and a {@code statusValue} gives the state it names. So the two
   * are both null, or both set and agree.
   *
   * <p>A {@code statusValue} also decides over {@link WorkflowState#UNKNOWN}, which says only that
   * whoever built the status did not know the value: a version that knows it reads it as itself.
   *
   * @throws IllegalArgumentException if {@code status} is {@link WorkflowState#UNKNOWN} without a
   *     {@code statusValue}, or a known {@code status} and the {@code statusValue} do not agree
   */
  public WorkflowStatus {
    if (statusValue == null && status != null && status != WorkflowState.UNKNOWN) {
      statusValue = status.name();
    }
    if (statusValue != null && (status == null || status == WorkflowState.UNKNOWN)) {
      status = WorkflowState.fromStatusValue(statusValue);
    }
    if (status == WorkflowState.UNKNOWN && statusValue == null) {
      throw new IllegalArgumentException(
          "Workflow %s: an UNKNOWN status needs the status value it stands for"
              .formatted(workflowId));
    }
    if (status != null
        && statusValue != null
        && status != WorkflowState.fromStatusValue(statusValue)) {
      throw new IllegalArgumentException(
          "Workflow %s: status %s does not match status value %s"
              .formatted(workflowId, status, statusValue));
    }
  }

  /**
   * Constructs a status whose stored status is the name of {@code status}.
   *
   * @throws IllegalArgumentException if {@code status} is {@link WorkflowState#UNKNOWN}, which
   *     needs the stored value it stands for
   */
  public WorkflowStatus(
      String workflowId,
      WorkflowState status,
      String workflowName,
      String className,
      String instanceName,
      String authenticatedUser,
      String assumedRole,
      List<String> authenticatedRoles,
      Object[] input,
      Object output,
      ErrorResult error,
      String executorId,
      Instant createdAt,
      Instant updatedAt,
      String appVersion,
      String appId,
      Integer recoveryAttempts,
      String queueName,
      Duration timeout,
      Instant deadline,
      Instant startedAt,
      String deduplicationId,
      Integer priority,
      String queuePartitionKey,
      String forkedFrom,
      String parentWorkflowId,
      Boolean wasForkedFrom,
      Instant delayUntil,
      Instant completedAt,
      String serialization,
      Map<String, Object> attributes,
      String scheduleName,
      String applicationName,
      Boolean isDebounced,
      Instant debounceDeadline) {
    this(
        workflowId,
        status,
        workflowName,
        className,
        instanceName,
        authenticatedUser,
        assumedRole,
        authenticatedRoles,
        input,
        output,
        error,
        executorId,
        createdAt,
        updatedAt,
        appVersion,
        appId,
        recoveryAttempts,
        queueName,
        timeout,
        deadline,
        startedAt,
        deduplicationId,
        priority,
        queuePartitionKey,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        delayUntil,
        completedAt,
        serialization,
        attributes,
        scheduleName,
        applicationName,
        isDebounced,
        debounceDeadline,
        null);
  }

  @JsonIgnore
  public Long timeoutMs() {
    return timeout == null ? null : timeout.toMillis();
  }

  @JsonIgnore
  public Long deadlineEpochMs() {
    return deadline == null ? null : deadline.toEpochMilli();
  }

  @JsonIgnore
  public Long createdAtEpochMs() {
    return createdAt == null ? null : createdAt.toEpochMilli();
  }

  @JsonIgnore
  public Long updatedAtEpochMs() {
    return updatedAt == null ? null : updatedAt.toEpochMilli();
  }

  @JsonIgnore
  public Long startedAtEpochMs() {
    return startedAt == null ? null : startedAt.toEpochMilli();
  }

  @JsonIgnore
  public Long delayUntilEpochMs() {
    return delayUntil == null ? null : delayUntil.toEpochMilli();
  }

  @JsonIgnore
  public Long completedAtEpochMs() {
    return completedAt == null ? null : completedAt.toEpochMilli();
  }

  @JsonIgnore
  public Long debounceDeadlineEpochMs() {
    return debounceDeadline == null ? null : debounceDeadline.toEpochMilli();
  }

  /**
   * Custom equals required because {@code input} is an {@code Object[]} whose default {@code
   * Objects.equals} falls back to reference equality. {@code authenticatedRoles} is now a {@code
   * List<String>} and uses value-based equality naturally.
   *
   * @param obj the object to compare
   * @return true if all fields are equal, false otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;

    WorkflowStatus that = (WorkflowStatus) obj;

    return java.util.Objects.equals(workflowId, that.workflowId)
        && java.util.Objects.equals(status, that.status)
        && java.util.Objects.equals(workflowName, that.workflowName)
        && java.util.Objects.equals(className, that.className)
        && java.util.Objects.equals(instanceName, that.instanceName)
        && java.util.Objects.equals(authenticatedUser, that.authenticatedUser)
        && java.util.Objects.equals(assumedRole, that.assumedRole)
        && java.util.Objects.equals(authenticatedRoles, that.authenticatedRoles)
        && java.util.Arrays.deepEquals(input, that.input)
        && java.util.Objects.equals(output, that.output)
        && java.util.Objects.equals(error, that.error)
        && java.util.Objects.equals(executorId, that.executorId)
        && java.util.Objects.equals(createdAt, that.createdAt)
        && java.util.Objects.equals(updatedAt, that.updatedAt)
        && java.util.Objects.equals(appVersion, that.appVersion)
        && java.util.Objects.equals(appId, that.appId)
        && java.util.Objects.equals(recoveryAttempts, that.recoveryAttempts)
        && java.util.Objects.equals(queueName, that.queueName)
        && java.util.Objects.equals(timeout, that.timeout)
        && java.util.Objects.equals(deadline, that.deadline)
        && java.util.Objects.equals(startedAt, that.startedAt)
        && java.util.Objects.equals(deduplicationId, that.deduplicationId)
        && java.util.Objects.equals(priority, that.priority)
        && java.util.Objects.equals(queuePartitionKey, that.queuePartitionKey)
        && java.util.Objects.equals(forkedFrom, that.forkedFrom)
        && java.util.Objects.equals(parentWorkflowId, that.parentWorkflowId)
        && java.util.Objects.equals(wasForkedFrom, that.wasForkedFrom)
        && java.util.Objects.equals(delayUntil, that.delayUntil)
        && java.util.Objects.equals(completedAt, that.completedAt)
        && java.util.Objects.equals(serialization, that.serialization)
        && java.util.Objects.equals(attributes, that.attributes)
        && java.util.Objects.equals(scheduleName, that.scheduleName)
        && java.util.Objects.equals(applicationName, that.applicationName)
        && java.util.Objects.equals(isDebounced, that.isDebounced)
        && java.util.Objects.equals(debounceDeadline, that.debounceDeadline)
        && java.util.Objects.equals(statusValue, that.statusValue);
  }

  /**
   * Custom hashCode required because {@code input} is an {@code Object[]} that needs {@code
   * Arrays.deepHashCode} for value-based hashing.
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        workflowId,
        status,
        workflowName,
        className,
        instanceName,
        authenticatedUser,
        assumedRole,
        authenticatedRoles,
        java.util.Arrays.deepHashCode(input),
        output,
        error,
        executorId,
        createdAt,
        updatedAt,
        appVersion,
        appId,
        recoveryAttempts,
        queueName,
        timeout,
        deadline,
        startedAt,
        deduplicationId,
        priority,
        queuePartitionKey,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        delayUntil,
        completedAt,
        serialization,
        attributes,
        scheduleName,
        applicationName,
        isDebounced,
        debounceDeadline,
        statusValue);
  }

  /**
   * The status as JSON writes it: the stored value, never {@code UNKNOWN}. An export or a recorded
   * step from this version then reads on a version that knows the status as that status.
   */
  @SuppressWarnings("PMD.UnusedPrivateMethod") // Jackson calls it
  @JsonProperty("status")
  @JsonSerialize(using = StoredStatusSerializer.class)
  private String statusForJson() {
    return statusValue;
  }

  /**
   * Writes the stored value as a {@link WorkflowState} would be written, type id included when the
   * mapper writes one, so it reads back through {@link WorkflowState#fromStatusValue}.
   */
  static final class StoredStatusSerializer extends StdScalarSerializer<String> {
    StoredStatusSerializer() {
      super(String.class);
    }

    @Override
    public void serialize(String value, JsonGenerator gen, SerializationContext ctxt) {
      gen.writeString(value);
    }

    @Override
    public void serializeWithType(
        String value, JsonGenerator gen, SerializationContext ctxt, TypeSerializer typeSer) {
      var typeId =
          typeSer.writeTypePrefix(
              gen, ctxt, typeSer.typeId(value, WorkflowState.class, JsonToken.VALUE_STRING));
      serialize(value, gen, ctxt);
      typeSer.writeTypeSuffix(gen, ctxt, typeId);
    }
  }
}
