package dev.dbos.transact.workflow;

import java.time.OffsetDateTime;
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
    List<String> workflowName,
    String className,
    String instanceName,
    List<String> applicationVersion,
    List<String> authenticatedUser,
    Integer limit,
    Integer offset,
    Boolean sortDesc,
    String workflowIdPrefix,
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

  // TODO: remove with methods

  /** Restrict the returned workflows to those on the specified `workflowIds` list */
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

  /**
   * Restrict listWorkflows results to a single workflow ID. Specifying `null` for workflowId
   * removes the filter.
   *
   * @param workflowId Workflow ID to use for filtering workflows
   * @return a new ListWorkflowsInput record with the workflowId list set
   */
  public ListWorkflowsInput withWorkflowId(String workflowId) {
    return withWorkflowIds(workflowId == null ? null : List.of(workflowId));
  }

  /** Restrict the returned workflows to those with a status on the specified list */
  public ListWorkflowsInput withStatuses(List<String> status) {
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

  /** Restrict the returned workflows to those with a status of `status` */
  public ListWorkflowsInput withStatus(String stat) {
    return withStatuses(stat == null ? null : List.of(stat));
  }

  /** Restrict the returned workflows to those with a status of `status` */
  public ListWorkflowsInput withStatus(WorkflowState stat) {
    return withStatuses(stat == null ? null : List.of(stat.name()));
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
  }

  /** Restrict the returned workflows to those with a function name on the specified list */
  public ListWorkflowsInput withWorkflowNames(List<String> workflowName) {
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

  /** Restrict the returned workflows to those with the function name `workflowName` */
  public ListWorkflowsInput withWorkflowName(String workflowName) {
    return withWorkflowNames(workflowName == null ? null : List.of(workflowName));
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
  }

  /** Restrict the returned workflows to those run on app versions on the specified list */
  public ListWorkflowsInput withApplicationVersions(List<String> applicationVersion) {
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

  /** Restrict the returned workflows to those run on app version `applicationVersion` */
  public ListWorkflowsInput withApplicationVersion(String applicationVersion) {
    return withApplicationVersions(applicationVersion == null ? null : List.of(applicationVersion));
  }

  /** Restrict the returned workflows to those run by users on the specified list */
  public ListWorkflowsInput withAuthenticatedUsers(List<String> authenticatedUser) {
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

  /** Restrict the returned workflows to those run by user `authenticatedUser` */
  public ListWorkflowsInput withAuthenticatedUser(String authenticatedUser) {
    return withAuthenticatedUsers(authenticatedUser == null ? null : List.of(authenticatedUser));
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
  }

  /** Restrict the returned workflows to those enqueued on queues on the specified list */
  public ListWorkflowsInput withQueueNames(List<String> queueName) {
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

  /**
   * Restrict the returned workflows to those enqueued on the queue named `queueName`. If `null`, no
   * restriction is applied.
   */
  public ListWorkflowsInput withQueueName(String queueName) {
    return withQueueNames(queueName == null ? null : List.of(queueName));
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
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
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
        executorIds,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        hasParent);
  }

  /**
   * Restrict the returned workflows to those enqueued for or run by the executor `executorId`.
   * Specifying `null` for executorId removes the filter.
   *
   * @param executorId Executor ID to use for filtering workflows
   * @return a new ListWorkflowsInput record with the executorIds list set
   */
  public ListWorkflowsInput withExecutorId(String executorId) {
    return withExecutorIds(executorId == null ? null : List.of(executorId));
  }

  /** Restrict the returned workflows to those forked from workflows on the specified list */
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

  /** Restrict the returned workflows to those forked from the specified workflow ID */
  public ListWorkflowsInput withForkedFrom(String forkedFrom) {
    return withForkedFrom(forkedFrom == null ? null : List.of(forkedFrom));
  }

  /** Restrict the returned workflows to those with a parent workflow ID on the specified list */
  public ListWorkflowsInput withParentWorkflowIds(List<String> parentWorkflowId) {
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

  /** Restrict the returned workflows to those with the specified parent workflow ID */
  public ListWorkflowsInput withParentWorkflowId(String parentWorkflowId) {
    return withParentWorkflowIds(parentWorkflowId == null ? null : List.of(parentWorkflowId));
  }

  public ListWorkflowsInput withAddedWorkflowId(String workflowId) {
    if (workflowId == null) return this;
    List<String> existing = this.workflowIds;
    List<String> ids =
        existing == null
            ? List.of(workflowId)
            : java.util.stream.Stream.concat(
                    existing.stream(), java.util.stream.Stream.of(workflowId))
                .toList();
    return withWorkflowIds(ids);
  }

  public ListWorkflowsInput withAddedStatus(String status) {
    if (status == null) return this;
    List<String> existing = this.status;
    List<String> sts =
        existing == null
            ? List.of(status)
            : java.util.stream.Stream.concat(existing.stream(), java.util.stream.Stream.of(status))
                .toList();
    return withStatuses(sts);
  }

  public ListWorkflowsInput withAddedStatus(WorkflowState status) {
    if (status == null) return this;
    return withAddedStatus(status.name());
  }

  public ListWorkflowsInput withAddedExecutorId(String executorId) {
    if (executorId == null) return this;
    List<String> existing = this.executorIds;
    List<String> ids =
        existing == null
            ? List.of(executorId)
            : java.util.stream.Stream.concat(
                    existing.stream(), java.util.stream.Stream.of(executorId))
                .toList();
    return withExecutorIds(ids);
  }

  /** Restrict the returned workflows to those that were forked from another workflow */
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

  /** Restrict the returned workflows to those that have a parent workflow */
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

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private List<String> workflowIds;
    private List<String> status;
    private OffsetDateTime startTime;
    private OffsetDateTime endTime;
    private List<String> workflowName;
    private String className;
    private String instanceName;
    private List<String> applicationVersion;
    private List<String> authenticatedUser;
    private Integer limit;
    private Integer offset;
    private Boolean sortDesc;
    private String workflowIdPrefix;
    private Boolean loadInput;
    private Boolean loadOutput;
    private List<String> queueName;
    private Boolean queuesOnly;
    private List<String> executorIds;
    private List<String> forkedFrom;
    private List<String> parentWorkflowId;
    private Boolean wasForkedFrom;
    private Boolean hasParent;

    public Builder workflowIds(List<String> workflowIds) {
      this.workflowIds = workflowIds;
      return this;
    }

    public Builder workflowId(String workflowId) {
      return workflowIds(workflowId == null ? null : List.of(workflowId));
    }

    public Builder status(List<String> status) {
      this.status = status;
      return this;
    }

    public Builder status(String stat) {
      return status(stat == null ? null : List.of(stat));
    }

    public Builder status(WorkflowState stat) {
      return status(stat == null ? null : List.of(stat.name()));
    }

    public Builder startTime(OffsetDateTime startTime) {
      this.startTime = startTime;
      return this;
    }

    public Builder endTime(OffsetDateTime endTime) {
      this.endTime = endTime;
      return this;
    }

    public Builder workflowName(List<String> workflowName) {
      this.workflowName = workflowName;
      return this;
    }

    public Builder workflowName(String workflowName) {
      return workflowName(workflowName == null ? null : List.of(workflowName));
    }

    public Builder className(String className) {
      this.className = className;
      return this;
    }

    public Builder instanceName(String instanceName) {
      this.instanceName = instanceName;
      return this;
    }

    public Builder applicationVersion(List<String> applicationVersion) {
      this.applicationVersion = applicationVersion;
      return this;
    }

    public Builder applicationVersion(String applicationVersion) {
      return applicationVersion(applicationVersion == null ? null : List.of(applicationVersion));
    }

    public Builder authenticatedUser(List<String> authenticatedUser) {
      this.authenticatedUser = authenticatedUser;
      return this;
    }

    public Builder authenticatedUser(String authenticatedUser) {
      return authenticatedUser(authenticatedUser == null ? null : List.of(authenticatedUser));
    }

    public Builder limit(Integer limit) {
      this.limit = limit;
      return this;
    }

    public Builder offset(Integer offset) {
      this.offset = offset;
      return this;
    }

    public Builder sortDesc(Boolean sortDesc) {
      this.sortDesc = sortDesc;
      return this;
    }

    public Builder workflowIdPrefix(String workflowIdPrefix) {
      this.workflowIdPrefix = workflowIdPrefix;
      return this;
    }

    public Builder loadInput(Boolean loadInput) {
      this.loadInput = loadInput;
      return this;
    }

    public Builder loadOutput(Boolean loadOutput) {
      this.loadOutput = loadOutput;
      return this;
    }

    public Builder queueName(List<String> queueName) {
      this.queueName = queueName;
      return this;
    }

    public Builder queueName(String queueName) {
      return queueName(queueName == null ? null : List.of(queueName));
    }

    public Builder queuesOnly(Boolean queuesOnly) {
      this.queuesOnly = queuesOnly;
      return this;
    }

    public Builder executorId(List<String> executorIds) {
      this.executorIds = executorIds;
      return this;
    }

    public Builder executorId(String executorId) {
      return executorId(executorId == null ? null : List.of(executorId));
    }

    public Builder forkedFrom(List<String> forkedFrom) {
      this.forkedFrom = forkedFrom;
      return this;
    }

    public Builder forkedFrom(String forkedFrom) {
      return forkedFrom(forkedFrom == null ? null : List.of(forkedFrom));
    }

    public Builder parentWorkflowId(List<String> parentWorkflowId) {
      this.parentWorkflowId = parentWorkflowId;
      return this;
    }

    public Builder parentWorkflowId(String parentWorkflowId) {
      return parentWorkflowId(parentWorkflowId == null ? null : List.of(parentWorkflowId));
    }

    public Builder wasForkedFrom(Boolean wasForkedFrom) {
      this.wasForkedFrom = wasForkedFrom;
      return this;
    }

    public Builder hasParent(Boolean hasParent) {
      this.hasParent = hasParent;
      return this;
    }

    public ListWorkflowsInput build() {
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
  }
}
