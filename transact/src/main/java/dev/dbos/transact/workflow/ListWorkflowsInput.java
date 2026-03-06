package dev.dbos.transact.workflow;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Argument to `DBOS.listWorkflows`, specifying the set of filters that can be applied to the
 * returned list of workflows. These include filtering based on IDs, ID prefixes, names, times,
 * status, queues, etc. Also, this structure controls whether the input and output are returned.
 */
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
    List<String> executorIds,
    String forkedFrom,
    String parentWorkflowId) {

  public ListWorkflowsInput() {
    this(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null);
  }

  /** Restrict the returned workflows to those on the specified `workflowIds` list */
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
        executorIds,
        forkedFrom,
        parentWorkflowId);
  }

  /**
   * Restrict listWorkflows results to a single workflow ID. Specifying `null` for workflowId
   * removes the filter.
   *
   * @param workflowId Workflow ID to use for filtering workflows
   * @return a new ListWorkflowsInput record with the workflowId list set
   */
  public ListWorkflowsInput withWorkflowId(String workflowId) {
    if (workflowId == null) return withWorkflowIds(null);

    return withWorkflowIds(List.of(workflowId));
  }

  /** Restrict the returned workflows to those with a status on the specified `status` list */
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
        executorIds,
        forkedFrom,
        parentWorkflowId);
  }

  /** Restrict the returned workflows to those with a status of `status` */
  public ListWorkflowsInput withStatus(String stat) {
    if (stat == null) return withStatuses(null);
    List<String> stats = new ArrayList<>();
    stats.add(stat);
    return withStatuses(stats);
  }

  /** Restrict the returned workflows to those with a status of `status` */
  public ListWorkflowsInput withStatus(WorkflowState stat) {
    if (stat == null) return withStatuses(null);
    List<String> stats = new ArrayList<>();
    stats.add(stat.name());
    return withStatuses(stats);
  }

  /** Restrict the returned workflows to those initiated on or after `startTime` */
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
        executorIds,
        forkedFrom,
        parentWorkflowId);
  }

  /** Restrict the returned workflows to those initiated on or before `endTime` */
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
        executorIds,
        forkedFrom,
        parentWorkflowId);
  }

  /** Restrict the returned workflows to those with the function name `workflowName` */
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
        executorIds,
        forkedFrom,
        parentWorkflowId);
  }

  /** Restrict the returned workflows to those within the class named `className` */
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
        parentWorkflowId);
  }

  /** Restrict the returned workflows to those within the instance named `instanceName` */
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
        parentWorkflowId);
  }

  /** Restrict the returned workflows to those run on app version `applicationVersion` */
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
        executorIds,
        forkedFrom,
        parentWorkflowId);
  }

  /** Restrict the returned workflows to those run by user `authenticatedUser` */
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
        executorIds,
        forkedFrom,
        parentWorkflowId);
  }

  /** Restrict the number of returned workflows to `limit` */
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
        parentWorkflowId);
  }

  /**
   * Restrict the set of workflows returned by starting from `offset` in the returned list; used in
   * conjunction with `limit`
   */
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
        parentWorkflowId);
  }

  /**
   * Allows the returned set of workflows to be sorted in descending order of creation, rather than
   * ascending.
   */
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
        parentWorkflowId);
  }

  /** Filter returned workflows by a prefix of the workflow ID */
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
        executorIds,
        forkedFrom,
        parentWorkflowId);
  }

  /** If true, workflow inputs will be materialized and returned as part of the record */
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
        parentWorkflowId);
  }

  /**
   * If true, workflow output (return value or error) will be materialized and returned as part of
   * the record
   */
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
        parentWorkflowId);
  }

  /**
   * Restrict the returned workflows to those enqueued on the queue named `queueName`. If `null`, no
   * restriction is applied.
   */
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
        executorIds,
        forkedFrom,
        parentWorkflowId);
  }

  /** Restrict the returned workflows to only those run on queues. */
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
        executorIds,
        forkedFrom,
        parentWorkflowId);
  }

  /**
   * Restrict the returned workflows to those enqueued for or run by an executor on the specified
   * list.
   */
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
        executorIds == null ? null : List.copyOf(executorIds),
        forkedFrom,
        parentWorkflowId);
  }

  /** Restrict the returned workflows to those forked from the specified workflow ID */
  public ListWorkflowsInput withForkedFrom(String forkedFrom) {
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
        parentWorkflowId);
  }

  /** Restrict the returned workflows to those with the specified parent workflow ID */
  public ListWorkflowsInput withParentWorkflowId(String parentWorkflowId) {
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
        parentWorkflowId);
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
}
