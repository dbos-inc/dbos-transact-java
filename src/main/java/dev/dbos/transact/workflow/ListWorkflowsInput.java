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
    return withWorkflowIds(stats);
  }

  public ListWorkflowsInput withStatus(WorkflowState stat) {
    if (stat == null) return withStatuses(null);
    List<String> stats = new ArrayList<>();
    stats.add(stat.name());
    return withWorkflowIds(stats);
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

  public static class Builder {
    private List<String> workflowIds = new ArrayList<>();
    private List<String> status = new ArrayList<>();
    private OffsetDateTime startTime;
    private OffsetDateTime endTime;
    private String workflowName;
    private String className;
    private String instanceName;
    private String authenticatedUser;
    private String applicationVersion;
    private Integer limit;
    private Integer offset;
    private Boolean sortDesc;
    private String workflowIdPrefix;
    private Boolean loadInput;
    private Boolean loadOutput;
    private String queueName;
    private Boolean queuedOnly;
    private List<String> executorIds = new ArrayList<>();

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
          queuedOnly,
          executorIds);
    }

    public Builder workflowId(String workflowId) {
      if (workflowId != null) {
        this.workflowIds.add(workflowId);
      }
      return this;
    }

    public Builder workflowIds(List<String> workflowIds) {
      if (workflowIds != null) {
        this.workflowIds.addAll(workflowIds);
      }
      return this;
    }

    public Builder status(WorkflowState status) {
      this.status.add(status.name());
      return this;
    }

    public Builder status(String status) {
      if (status != null) {
        this.status.add(status);
      }
      return this;
    }

    public Builder startTime(OffsetDateTime startTime) {
      this.startTime = startTime;
      return this;
    }

    public Builder endTime(OffsetDateTime endTime) {
      this.endTime = endTime;
      return this;
    }

    public Builder workflowName(String workflowName) {
      this.workflowName = workflowName;
      return this;
    }

    public Builder className(String className) {
      this.className = className;
      return this;
    }

    public Builder instanceName(String instanceName) {
      this.instanceName = instanceName;
      return this;
    }

    public Builder applicationVersion(String applicationVersion) {
      this.applicationVersion = applicationVersion;
      return this;
    }

    public Builder authenticatedUser(String authenticatedUser) {
      this.authenticatedUser = authenticatedUser;
      return this;
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

    public Builder loadInput(Boolean value) {
      this.loadInput = value;
      return this;
    }

    public Builder loadOutput(Boolean value) {
      this.loadOutput = value;
      return this;
    }

    public Builder queueName(String queueName) {
      this.queueName = queueName;
      return this;
    }

    public Builder queuedOnly(Boolean queuedOnly) {
      this.queuedOnly = queuedOnly;
      return this;
    }

    public Builder executorId(String executorId) {
      if (executorId != null) {
        this.executorIds.add(executorId);
      }
      return this;
    }

    public Builder executorIds(List<String> executorIds) {
      if (executorIds != null) {
        this.executorIds.addAll(executorIds);
      }
      return this;
    }
  }
}
