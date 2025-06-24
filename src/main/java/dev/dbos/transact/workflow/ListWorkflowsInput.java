package dev.dbos.transact.workflow;

import java.util.List;
import java.time.OffsetDateTime;

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

    public ListWorkflowsInput() {
    }

    public ListWorkflowsInput(
            List<String> workflowIDs, String workflowName, String authenticatedUser,
            OffsetDateTime startTime, OffsetDateTime endTime, String status,
            String applicationVersion, Integer limit, Integer offset, Boolean sortDesc,
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

    public List<String> getWorkflowIDs() { return workflowIDs; }
    public void setWorkflowIDs(List<String> workflowIDs) { this.workflowIDs = workflowIDs; }

    public String getWorkflowName() { return workflowName; }
    public void setWorkflowName(String workflowName) { this.workflowName = workflowName; }

    public String getAuthenticatedUser() { return authenticatedUser; }
    public void setAuthenticatedUser(String authenticatedUser) { this.authenticatedUser = authenticatedUser; }

    public OffsetDateTime getStartTime() { return startTime; }
    public void setStartTime(OffsetDateTime startTime) { this.startTime = startTime; }

    public OffsetDateTime getEndTime() { return endTime; }
    public void setEndTime(OffsetDateTime endTime) { this.endTime = endTime; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getApplicationVersion() { return applicationVersion; }
    public void setApplicationVersion(String applicationVersion) { this.applicationVersion = applicationVersion; }

    public Integer getLimit() { return limit; }
    public void setLimit(Integer limit) { this.limit = limit; }

    public Integer getOffset() { return offset; }
    public void setOffset(Integer offset) { this.offset = offset; }

    public Boolean getSortDesc() { return sortDesc; }
    public void setSortDesc(Boolean sortDesc) { this.sortDesc = sortDesc; }

    public String getWorkflowIdPrefix() { return workflowIdPrefix; }
    public void setWorkflowIdPrefix(String workflowIdPrefix) { this.workflowIdPrefix = workflowIdPrefix; }
}
