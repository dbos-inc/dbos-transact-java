package dev.dbos.transact.workflow;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public record ListWorkflowsInput(
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
    String workflowIdPrefix,
    Boolean loadInput,
    Boolean loadOutput) {

  public ListWorkflowsInput() {
    this(null, null, null, null, null, null, null, null, null, null, null, null, null);
  }
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
    private Boolean loadInput;
    private Boolean loadOutput;

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

    public Builder loadInput(Boolean value) {
      this.loadInput = value;
      return this;
    }

    public Builder loadOutput(Boolean value) {
      this.loadOutput = value;
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
          workflowIdPrefix,
          loadInput,
          loadOutput);
    }
  }

}
