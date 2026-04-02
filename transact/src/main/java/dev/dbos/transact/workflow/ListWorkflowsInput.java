package dev.dbos.transact.workflow;

import java.time.OffsetDateTime;
import java.util.Arrays;

/**
 * Argument to `DBOS.listWorkflows`, specifying the set of filters that can be applied to the
 * returned list of workflows. These include filtering based on IDs, ID prefixes, names, times,
 * status, queues, etc. Also, this structure controls whether the input and output are returned.
 */
public record ListWorkflowsInput(
    String[] workflowIds,
    String[] status,
    OffsetDateTime startTime,
    OffsetDateTime endTime,
    String[] workflowName,
    String className,
    String instanceName,
    String[] applicationVersion,
    String[] authenticatedUser,
    Integer limit,
    Integer offset,
    Boolean sortDesc,
    String workflowIdPrefix,
    Boolean loadInput,
    Boolean loadOutput,
    String[] queueName,
    Boolean queuesOnly,
    String[] executorIds,
    String[] forkedFrom,
    String[] parentWorkflowId) {

  public ListWorkflowsInput() {
    this(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null);
  }

  /** Restrict the returned workflows to those on the specified `workflowIds` list */
  public ListWorkflowsInput withWorkflowIds(String[] workflowIds) {
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
   * Restrict listWorkflows results to a single workflow ID. Specifying `null` for workflowId
   * removes the filter.
   *
   * @param workflowId Workflow ID to use for filtering workflows
   * @return a new ListWorkflowsInput record with the workflowId list set
   */
  public ListWorkflowsInput withWorkflowId(String workflowId) {
    return withWorkflowIds(workflowId == null ? null : new String[] {workflowId});
  }

  /** Restrict the returned workflows to those with a status on the specified list */
  public ListWorkflowsInput withStatuses(String[] status) {
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

  /** Restrict the returned workflows to those with a status of `status` */
  public ListWorkflowsInput withStatus(String stat) {
    return withStatuses(stat == null ? null : new String[] {stat});
  }

  /** Restrict the returned workflows to those with a status of `status` */
  public ListWorkflowsInput withStatus(WorkflowState stat) {
    return withStatuses(stat == null ? null : new String[] {stat.name()});
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

  /** Restrict the returned workflows to those with a function name on the specified list */
  public ListWorkflowsInput withWorkflowNames(String[] workflowName) {
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
    return withWorkflowNames(workflowName == null ? null : new String[] {workflowName});
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

  /** Restrict the returned workflows to those run on app versions on the specified list */
  public ListWorkflowsInput withApplicationVersions(String[] applicationVersion) {
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
    return withApplicationVersions(
        applicationVersion == null ? null : new String[] {applicationVersion});
  }

  /** Restrict the returned workflows to those run by users on the specified list */
  public ListWorkflowsInput withAuthenticatedUsers(String[] authenticatedUser) {
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
    return withAuthenticatedUsers(
        authenticatedUser == null ? null : new String[] {authenticatedUser});
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

  /** Restrict the returned workflows to those enqueued on queues on the specified list */
  public ListWorkflowsInput withQueueNames(String[] queueName) {
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
    return withQueueNames(queueName == null ? null : new String[] {queueName});
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
  public ListWorkflowsInput withExecutorIds(String[] executorIds) {
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
   * Restrict the returned workflows to those enqueued for or run by the executor `executorId`.
   * Specifying `null` for executorId removes the filter.
   *
   * @param executorId Executor ID to use for filtering workflows
   * @return a new ListWorkflowsInput record with the executorIds list set
   */
  public ListWorkflowsInput withExecutorId(String executorId) {
    return withExecutorIds(executorId == null ? null : new String[] {executorId});
  }

  /** Restrict the returned workflows to those forked from workflows on the specified list */
  public ListWorkflowsInput withForkedFrom(String[] forkedFrom) {
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

  /** Restrict the returned workflows to those forked from the specified workflow ID */
  public ListWorkflowsInput withForkedFrom(String forkedFrom) {
    return withForkedFrom(forkedFrom == null ? null : new String[] {forkedFrom});
  }

  /** Restrict the returned workflows to those with a parent workflow ID on the specified list */
  public ListWorkflowsInput withParentWorkflowIds(String[] parentWorkflowId) {
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
    return withParentWorkflowIds(parentWorkflowId == null ? null : new String[] {parentWorkflowId});
  }

  public ListWorkflowsInput withAddedWorkflowId(String workflowId) {
    if (workflowId == null) return this;
    String[] existing = this.workflowIds;
    String[] ids = existing == null ? new String[1] : Arrays.copyOf(existing, existing.length + 1);
    ids[ids.length - 1] = workflowId;
    return withWorkflowIds(ids);
  }

  public ListWorkflowsInput withAddedStatus(String status) {
    if (status == null) return this;
    String[] existing = this.status;
    String[] sts = existing == null ? new String[1] : Arrays.copyOf(existing, existing.length + 1);
    sts[sts.length - 1] = status;
    return withStatuses(sts);
  }

  public ListWorkflowsInput withAddedStatus(WorkflowState status) {
    if (status == null) return this;
    return withAddedStatus(status.name());
  }

  public ListWorkflowsInput withAddedExecutorId(String executorId) {
    if (executorId == null) return this;
    String[] existing = this.executorIds;
    String[] ids = existing == null ? new String[1] : Arrays.copyOf(existing, existing.length + 1);
    ids[ids.length - 1] = executorId;
    return withExecutorIds(ids);
  }
}
