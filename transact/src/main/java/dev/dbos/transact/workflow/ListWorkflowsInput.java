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
    List<String> status,
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

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private List<String> workflowIds;
    private List<String> status;
    private Instant startTime;
    private Instant endTime;
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

    public Builder startTime(Instant startTime) {
      this.startTime = startTime;
      return this;
    }

    public Builder endTime(Instant endTime) {
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
