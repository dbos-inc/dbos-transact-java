package dev.dbos.transact.workflow;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public class ListWorkflowsInput {
  private List<String> workflowIDs;
  private String workflowName;
  private String authenticatedUser;
  private OffsetDateTime startTime;
  private OffsetDateTime endTime;
  private String status;
  private String applicationVersion;
  private Integer limit;
  private Integer offset;
  private Boolean sortDesc;
  private String workflowIdPrefix;

  public ListWorkflowsInput() {}

  public static class Builder {
    private List<String> workflowIDs = new ArrayList<>();
    private String workflowName;
    private String authenticatedUser;
    private OffsetDateTime startTime;
    private OffsetDateTime endTime;
    private String status;
    private String applicationVersion;
    private Integer limit;
    private Integer offset;
    private Boolean sortDesc;
    private String workflowIdPrefix;

    public Builder workflowID(String workflowID) {
      this.workflowIDs.add(workflowID);
      return this;
    }

    public Builder workflowIDs(List<String> workflowIDs) {
      this.workflowIDs.addAll(workflowIDs);
      return this;
    }

    public Builder workflowName(String workflowName) {
      this.workflowName = workflowName;
      return this;
    }

    public Builder authenticatedUser(String authenticatedUser) {
      this.authenticatedUser = authenticatedUser;
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

    public Builder status(WorkflowState status) {
      this.status = status.toString();
      return this;
    }

    public Builder status(String status) {
      this.status = status;
      return this;
    }

    public Builder applicationVersion(String applicationVersion) {
      this.applicationVersion = applicationVersion;
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

    public ListWorkflowsInput build() {
      return new ListWorkflowsInput(
          workflowIDs,
          workflowName,
          authenticatedUser,
          startTime,
          endTime,
          status,
          applicationVersion,
          limit,
          offset,
          sortDesc,
          workflowIdPrefix);
    }
  }

  public ListWorkflowsInput(
      List<String> workflowIDs,
      String workflowName,
      String authenticatedUser,
      OffsetDateTime startTime,
      OffsetDateTime endTime,
      String status,
      String applicationVersion,
      Integer limit,
      Integer offset,
      Boolean sortDesc,
      String workflowIdPrefix) {
    this.workflowIDs = workflowIDs;
    this.workflowName = workflowName;
    this.authenticatedUser = authenticatedUser;
    this.startTime = startTime;
    this.endTime = endTime;
    this.status = status;
    this.applicationVersion = applicationVersion;
    this.limit = limit;
    this.offset = offset;
    this.sortDesc = sortDesc;
    this.workflowIdPrefix = workflowIdPrefix;
  }

  public List<String> getWorkflowIDs() {
    return workflowIDs;
  }

  public void setWorkflowIDs(List<String> workflowIDs) {
    this.workflowIDs = workflowIDs;
  }

  public String getWorkflowName() {
    return workflowName;
  }

  public void setWorkflowName(String workflowName) {
    this.workflowName = workflowName;
  }

  public String getAuthenticatedUser() {
    return authenticatedUser;
  }

  public void setAuthenticatedUser(String authenticatedUser) {
    this.authenticatedUser = authenticatedUser;
  }

  public OffsetDateTime getStartTime() {
    return startTime;
  }

  public void setStartTime(OffsetDateTime startTime) {
    this.startTime = startTime;
  }

  public OffsetDateTime getEndTime() {
    return endTime;
  }

  public void setEndTime(OffsetDateTime endTime) {
    this.endTime = endTime;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getApplicationVersion() {
    return applicationVersion;
  }

  public void setApplicationVersion(String applicationVersion) {
    this.applicationVersion = applicationVersion;
  }

  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  public Integer getOffset() {
    return offset;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }

  public Boolean getSortDesc() {
    return sortDesc;
  }

  public void setSortDesc(Boolean sortDesc) {
    this.sortDesc = sortDesc;
  }

  public String getWorkflowIdPrefix() {
    return workflowIdPrefix;
  }

  public void setWorkflowIdPrefix(String workflowIdPrefix) {
    this.workflowIdPrefix = workflowIdPrefix;
  }
}
