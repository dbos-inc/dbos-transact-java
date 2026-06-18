package dev.dbos.transact.context;

import static dev.dbos.transact.internal.Validation.nullableIsEmpty;
import static dev.dbos.transact.internal.Validation.nullableIsNotPositive;
import static dev.dbos.transact.internal.Validation.validateAttributes;

import dev.dbos.transact.workflow.Field;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The WorkflowOptions class is used to specify options for DBOS workflow functions that are invoked
 * synchronously. For example, the following construct will run a workflow under id `wfId`, and
 * restore the context when complete: try (var _i = new WorkflowOptions(wfId).setContext()) { ...
 * function called here will get id `wfId` ... }
 *
 * @param workflowId The ID to be assigned to the next workflow in the DBOS context
 * @param timeout The timeout to be assigned to the next workflow in the DBOS context
 * @param deadline The deadline to be assigned to the next workflow in the DBOS context
 * @param attributes Custom JSON-serializable attributes to attach to the next workflow. Recorded in
 *     the workflow status at creation and not inherited by child workflows.
 */
public record WorkflowOptions(
    @Nullable String workflowId,
    @Nullable Timeout timeout,
    @Nullable Instant deadline,
    @NonNull Field<String> authenticatedUser,
    @NonNull Field<String> assumedRole,
    @NonNull Field<List<String>> authenticatedRoles,
    @Nullable Map<String, Object> attributes) {

  public WorkflowOptions {
    if (nullableIsEmpty(workflowId)) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    if (timeout instanceof Timeout.Explicit explicit && nullableIsNotPositive(explicit.value())) {
      throw new IllegalArgumentException("explicit timeout must be a positive non-zero duration");
    }

    if (authenticatedRoles instanceof Field.Present<List<String>> p && p.value() != null) {
      authenticatedRoles = Field.of(List.copyOf(p.value()));
    }

    attributes = validateAttributes(attributes);
  }

  /** Create a WorkflowOptions with no ID and no timout */
  public WorkflowOptions() {
    this(null, null, null, Field.absent(), Field.absent(), Field.absent(), null);
  }

  /** Create a WorkflowOptions with a specified workflow ID and no timout */
  public WorkflowOptions(@Nullable String workflowId) {
    this(workflowId, null, null, Field.absent(), Field.absent(), Field.absent(), null);
  }

  /** Create a workflow options like this one, but with the workflowId set */
  public @NonNull WorkflowOptions withWorkflowId(@Nullable String workflowId) {
    return new WorkflowOptions(
        workflowId,
        this.timeout,
        this.deadline,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes);
  }

  /**
   * Create a WorkflowOptions like this one, but with the timeout set
   *
   * @param timeout timeout to use, expressed as a `dev.dbos.transact.workflow.Timeout`
   */
  public @NonNull WorkflowOptions withTimeout(@Nullable Timeout timeout) {
    return new WorkflowOptions(
        this.workflowId,
        timeout,
        this.deadline,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes);
  }

  /**
   * Create a WorkflowOptions like this one, but with the timeout set
   *
   * @param timeout timeout to use, expressed as a `java.util.Duration`
   */
  public @NonNull WorkflowOptions withTimeout(@NonNull Duration timeout) {
    return withTimeout(Timeout.of(timeout));
  }

  /**
   * Create a WorkflowOptions like this one, but with the timeout set
   *
   * @param value timeout value to use, expressed as a value (see `unit`)
   * @param unit units to use for interpreting timeout `value`
   */
  public @NonNull WorkflowOptions withTimeout(long value, @NonNull TimeUnit unit) {
    return withTimeout(Duration.ofNanos(unit.toNanos(value)));
  }

  /**
   * Create a WorkflowOptions like this one, but with the deadline set
   *
   * @param deadline deadline to use, expressed as a `java.util.Instant`
   */
  public @NonNull WorkflowOptions withDeadline(@Nullable Instant deadline) {
    return new WorkflowOptions(
        this.workflowId,
        this.timeout,
        deadline,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes);
  }

  /** Create a workflow options like this one, but without a timeout */
  public @NonNull WorkflowOptions withNoTimeout() {
    return new WorkflowOptions(
        this.workflowId,
        Timeout.none(),
        this.deadline,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes);
  }

  /** Create a WorkflowOptions like this one, but with the authenticated user set */
  public @NonNull WorkflowOptions withAuthenticatedUser(@Nullable String authenticatedUser) {
    return new WorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        Field.of(authenticatedUser),
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes);
  }

  /** Create a WorkflowOptions like this one, but with the authenticated user cleared */
  public @NonNull WorkflowOptions withNoAuthenticatedUser() {
    return new WorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        Field.of(null),
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes);
  }

  /** Create a WorkflowOptions like this one, but with the assumed role set */
  public @NonNull WorkflowOptions withAssumedRole(@Nullable String assumedRole) {
    return new WorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.authenticatedUser,
        Field.of(assumedRole),
        this.authenticatedRoles,
        this.attributes);
  }

  /** Create a WorkflowOptions like this one, but with the assumed role cleared */
  public @NonNull WorkflowOptions withNoAssumedRole() {
    return new WorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.authenticatedUser,
        Field.of(null),
        this.authenticatedRoles,
        this.attributes);
  }

  /** Create a WorkflowOptions like this one, but with the authenticated roles set */
  public @NonNull WorkflowOptions withAuthenticatedRoles(@Nullable String... authenticatedRoles) {
    return new WorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.authenticatedUser,
        this.assumedRole,
        authenticatedRoles != null ? Field.of(List.of(authenticatedRoles)) : Field.of(null),
        this.attributes);
  }

  /** Create a WorkflowOptions like this one, but with the authenticated roles cleared */
  public @NonNull WorkflowOptions withNoAuthenticatedRoles() {
    return new WorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.authenticatedUser,
        this.assumedRole,
        Field.of(null),
        this.attributes);
  }

  /** Create a WorkflowOptions like this one, but with the authenticated user and roles set */
  public @NonNull WorkflowOptions withAuthentication(
      @Nullable String authenticatedUser, @Nullable String... authenticatedRoles) {
    return new WorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        Field.of(authenticatedUser),
        this.assumedRole,
        authenticatedRoles != null ? Field.of(List.of(authenticatedRoles)) : Field.of(null),
        this.attributes);
  }

  /** Create a WorkflowOptions like this one, but with the authenticated user and roles cleared */
  public @NonNull WorkflowOptions withNoAuthentication() {
    return new WorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        Field.of(null),
        Field.of(null),
        Field.of(null),
        this.attributes);
  }

  /**
   * Create a WorkflowOptions like this one, but with the custom workflow attributes set. Attributes
   * must be JSON-serializable; they are recorded in the workflow status at creation and are not
   * inherited by child workflows.
   *
   * <p>Pass an empty map to clear attributes. Passing {@code null} leaves any attributes set by an
   * enclosing block in effect: within a nested {@code WorkflowOptions} block, {@code null} does not
   * override the outer block's attributes, whereas an empty map does (and is recorded as no
   * attributes).
   *
   * @param attributes custom attributes to attach to the next workflow, or an empty map to clear
   */
  public @NonNull WorkflowOptions withAttributes(@Nullable Map<String, Object> attributes) {
    return new WorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        attributes);
  }

  /**
   * Set the workflow options contained in this `WorkflowOptions` into the current DBOS context.
   * Should be called as an AutoCloseable so that the context is restored at the end of the block.
   * try (var _i = new WorkflowOptions(...).setContext()) { ... }
   */
  public @NonNull Guard setContext() {
    if (timeout instanceof Timeout.Explicit && deadline != null) {
      throw new IllegalArgumentException(
          "WorkflowOptions explicit timeout and deadline cannot both be set");
    }

    var ctx = DBOSContextHolder.get();
    var guard = new Guard(ctx);

    if (workflowId != null) {
      ctx.nextWorkflowId = workflowId;
    }
    if (timeout != null) {
      ctx.nextTimeout = timeout;
    }
    if (deadline != null) {
      ctx.nextDeadline = deadline;
    }
    if (authenticatedUser.isPresent()) {
      ctx.nextAuthenticatedUser = authenticatedUser;
    }
    if (assumedRole.isPresent()) {
      ctx.nextAssumedRole = assumedRole;
    }
    if (authenticatedRoles.isPresent()) {
      ctx.nextAuthenticatedRoles = authenticatedRoles;
    }
    if (attributes != null) {
      ctx.nextAttributes = attributes;
    }

    return guard;
  }

  public static class Guard implements AutoCloseable {

    private final DBOSContext ctx;
    private final String nextWorkflowId;
    private final Timeout timeout;
    private final Instant deadline;
    private final Field<String> authenticatedUser;
    private final Field<String> assumedRole;
    private final Field<List<String>> authenticatedRoles;
    private final Map<String, Object> attributes;

    private Guard(@NonNull DBOSContext ctx) {
      this.ctx = ctx;
      this.nextWorkflowId = ctx.nextWorkflowId;
      this.timeout = ctx.nextTimeout;
      this.deadline = ctx.nextDeadline;
      this.authenticatedUser = ctx.nextAuthenticatedUser;
      this.assumedRole = ctx.nextAssumedRole;
      this.authenticatedRoles = ctx.nextAuthenticatedRoles;
      this.attributes = ctx.nextAttributes;
    }

    @Override
    public void close() {
      ctx.nextWorkflowId = nextWorkflowId;
      ctx.nextTimeout = timeout;
      ctx.nextDeadline = deadline;
      ctx.nextAuthenticatedUser = authenticatedUser;
      ctx.nextAssumedRole = assumedRole;
      ctx.nextAuthenticatedRoles = authenticatedRoles;
      ctx.nextAttributes = attributes;
    }
  }
}
