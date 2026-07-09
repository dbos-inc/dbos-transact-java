package dev.dbos.transact.workflow;

import static dev.dbos.transact.internal.Validation.validateAttributes;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Argument to `DBOS.listWorkflows`, specifying the set of filters that can be applied to the
 * returned list of workflows. These include filtering based on IDs, ID prefixes, names, times,
 * status, queues, etc. Also, this structure controls whether the input and output are returned.
 */
public record ListWorkflowsInput(
    List<String> workflowIds,
    List<WorkflowState> status,
    Instant startTime,
    Instant endTime,
    List<String> workflowName,
    String className,
    String instanceName,
    List<String> applicationVersion,
    List<String> authenticatedUser,
    Integer limit,
    Integer offset,
    Boolean sortDesc,
    List<String> workflowIdPrefix,
    Boolean loadInput,
    Boolean loadOutput,
    List<String> queueName,
    Boolean queuesOnly,
    List<String> executorIds,
    List<String> forkedFrom,
    List<String> parentWorkflowId,
    Boolean wasForkedFrom,
    Boolean hasParent,
    Map<String, Object> attributes,
    Instant completedAfter,
    Instant completedBefore,
    Instant dequeuedAfter,
    Instant dequeuedBefore,
    List<String> scheduleName) {

  // Validate the attributes filter here (every convenience constructor, withX copy, and the
  // conductor/admin asInput() paths route through this canonical constructor) so an invalid filter
  // fails fast with a clear IllegalArgumentException rather than opaquely in WorkflowDAO's toJson.
  public ListWorkflowsInput {
    attributes = validateAttributes(attributes);
  }

  public ListWorkflowsInput() {
    this(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null);
  }

  public ListWorkflowsInput(String workflowId) {
    this(List.of(workflowId));
  }

  public ListWorkflowsInput(List<String> workflowIds) {
    this(
        workflowIds,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  // With methods for immutable updates
  public ListWorkflowsInput withWorkflowIds(List<String> workflowIds) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withStatus(List<WorkflowState> status) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withStartTime(Instant startTime) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withEndTime(Instant endTime) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withWorkflowName(List<String> workflowName) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withClassName(String className) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withInstanceName(String instanceName) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withApplicationVersion(List<String> applicationVersion) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withAuthenticatedUser(List<String> authenticatedUser) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withLimit(Integer limit) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withOffset(Integer offset) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withSortDesc(Boolean sortDesc) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withWorkflowIdPrefix(List<String> workflowIdPrefix) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withLoadInput(Boolean loadInput) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withLoadOutput(Boolean loadOutput) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withQueueName(List<String> queueName) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withQueuesOnly(Boolean queuesOnly) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withExecutorIds(List<String> executorIds) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withForkedFrom(List<String> forkedFrom) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withParentWorkflowId(List<String> parentWorkflowId) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withWasForkedFrom(Boolean wasForkedFrom) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withHasParent(Boolean hasParent) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withAttributes(Map<String, Object> attributes) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withCompletedAfter(Instant completedAfter) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withCompletedBefore(Instant completedBefore) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withDequeuedAfter(Instant dequeuedAfter) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withDequeuedBefore(Instant dequeuedBefore) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  public ListWorkflowsInput withScheduleName(List<String> scheduleName) {
    return new ListWorkflowsInput(
        workflowIds,
        status,
        startTime,
        endTime,
        workflowName,
        className,
        instanceName,
        applicationVersion,
        authenticatedUser,
        limit,
        offset,
        sortDesc,
        workflowIdPrefix,
        loadInput,
        loadOutput,
        queueName,
        queuesOnly,
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent,
        attributes,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        scheduleName);
  }

  // Single value overloads for list parameters
  public ListWorkflowsInput withWorkflowIds(String workflowId) {
    return withWorkflowIds(List.of(workflowId));
  }

  public ListWorkflowsInput withStatus(WorkflowState... status) {
    return withStatus(Arrays.asList(status));
  }

  public ListWorkflowsInput withWorkflowName(String workflowName) {
    return withWorkflowName(List.of(workflowName));
  }

  public ListWorkflowsInput withApplicationVersion(String applicationVersion) {
    return withApplicationVersion(List.of(applicationVersion));
  }

  public ListWorkflowsInput withAuthenticatedUser(String authenticatedUser) {
    return withAuthenticatedUser(List.of(authenticatedUser));
  }

  public ListWorkflowsInput withWorkflowIdPrefix(String workflowIdPrefix) {
    return withWorkflowIdPrefix(List.of(workflowIdPrefix));
  }

  public ListWorkflowsInput withQueueName(String queueName) {
    return withQueueName(List.of(queueName));
  }

  public ListWorkflowsInput withExecutorIds(String executorId) {
    return withExecutorIds(List.of(executorId));
  }

  public ListWorkflowsInput withForkedFrom(String forkedFrom) {
    return withForkedFrom(List.of(forkedFrom));
  }

  public ListWorkflowsInput withParentWorkflowId(String parentWorkflowId) {
    return withParentWorkflowId(List.of(parentWorkflowId));
  }

  public ListWorkflowsInput withScheduleName(String scheduleName) {
    return withScheduleName(List.of(scheduleName));
  }
}
