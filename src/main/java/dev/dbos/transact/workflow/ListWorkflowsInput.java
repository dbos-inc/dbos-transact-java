package dev.dbos.transact.workflow;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public record ListWorkflowsInput(
    List<String> workflowIds,
    List<String> status,
    OffsetDateTime startTime,
    OffsetDateTime endTime,
    String workflowName,
    String className,
    String instanceName,
    String applicationVersion,
    String authenticatedUser,
    Integer limit,
    Integer offset,
    Boolean sortDesc,
    String workflowIdPrefix,
    Boolean loadInput,
    Boolean loadOutput,
    String queueName,
    Boolean queuesOnly,
    List<String> executorIds) {

  public ListWorkflowsInput() {
    this(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null);
  }

  public ListWorkflowsInput withWorkflowIds(List<String> workflowIds) {
    return new ListWorkflowsInput(
        workflowIds == null ? null : List.copyOf(workflowIds),
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
        executorIds);
  }

  public ListWorkflowsInput withWorkflowId(String workflowId) {
    if (workflowId == null) return withWorkflowIds(null);

    List<String> ids = new ArrayList<>();
    ids.add(workflowId);
    return withWorkflowIds(ids);
  }

  public ListWorkflowsInput withStatuses(List<String> status) {
    return new ListWorkflowsInput(
        workflowIds,
        status == null ? null : List.copyOf(status),
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
        executorIds);
  }

  public ListWorkflowsInput withStatus(String stat) {
    if (stat == null) return withStatuses(null);
    List<String> stats = new ArrayList<>();
    stats.add(stat);
    return withStatuses(stats);
  }

  public ListWorkflowsInput withStatus(WorkflowState stat) {
    if (stat == null) return withStatuses(null);
    List<String> stats = new ArrayList<>();
    stats.add(stat.name());
    return withStatuses(stats);
  }

  public ListWorkflowsInput withStartTime(OffsetDateTime startTime) {
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
        executorIds);
  }

  public ListWorkflowsInput withEndTime(OffsetDateTime endTime) {
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
        executorIds);
  }

  public ListWorkflowsInput withWorkflowName(String workflowName) {
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
        executorIds);
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
        executorIds);
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
        executorIds);
  }

  public ListWorkflowsInput withApplicationVersion(String applicationVersion) {
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
        executorIds);
  }

  public ListWorkflowsInput withAuthenticatedUser(String authenticatedUser) {
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
        executorIds);
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
        executorIds);
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
        executorIds);
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
        executorIds);
  }

  public ListWorkflowsInput withWorkflowIdPrefix(String workflowIdPrefix) {
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
        executorIds);
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
        executorIds);
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
        executorIds);
  }

  public ListWorkflowsInput withQueueName(String queueName) {
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
        executorIds);
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
        executorIds == null ? null : List.copyOf(executorIds));
  }

  public ListWorkflowsInput withAddedWorkflowId(String workflowId) {
    if (workflowId == null) return this;
    List<String> ids =
        this.workflowIds == null ? new ArrayList<>() : new ArrayList<>(this.workflowIds);
    ids.add(workflowId);
    return withWorkflowIds(ids);
  }

  public ListWorkflowsInput withAddedStatus(String status) {
    if (status == null) return this;
    List<String> sts = this.status == null ? new ArrayList<>() : new ArrayList<>(this.status);
    sts.add(status);
    return withStatuses(sts);
  }

  public ListWorkflowsInput withAddedStatus(WorkflowState status) {
    if (status == null) return this;
    return withAddedStatus(status.name());
  }

  public ListWorkflowsInput withAddedExecutorId(String executorId) {
    if (executorId == null) return this;
    List<String> ids =
        this.executorIds == null ? new ArrayList<>() : new ArrayList<>(this.executorIds);
    ids.add(executorId);
    return withExecutorIds(ids);
  }

  public ListWorkflowsInput withQueuesOnly() {
    return withQueuesOnly(true);
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
        executorIds);
  }
}
