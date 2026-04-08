package dev.dbos.transact.workflow;

import java.time.Instant;
import java.util.List;

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
    Boolean hasParent) {

  public ListWorkflowsInput() {
    this(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
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
        hasParent);
  }

  // Single value overloads for list parameters
  public ListWorkflowsInput withWorkflowIds(String workflowId) {
    return withWorkflowIds(List.of(workflowId));
  }

  public ListWorkflowsInput withStatus(WorkflowState status) {
    return withStatus(List.of(status));
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
}
