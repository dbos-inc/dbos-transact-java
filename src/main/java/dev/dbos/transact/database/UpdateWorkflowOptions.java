package dev.dbos.transact.database;

public class UpdateWorkflowOptions {

    private String output;
    private String error;
    private boolean resetRecoveryAttempts;
    private String queueName;
    private Boolean resetDeadline;
    private Boolean resetDeduplicationID;
    private Boolean resetStartedAtEpochMs;

    private String whereStatus;
    private boolean throwOnFailure;

    public UpdateWorkflowOptions() {
    }


    public UpdateWorkflowOptions withOutput(String output) {
        this.output = output;
        return this;
    }

    public UpdateWorkflowOptions withError(String error) {
        this.error = error;
        return this;
    }

    public UpdateWorkflowOptions withResetRecoveryAttempts(Boolean resetRecoveryAttempts) {
        this.resetRecoveryAttempts = resetRecoveryAttempts;
        return this;
    }

    public UpdateWorkflowOptions withQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public UpdateWorkflowOptions withResetDeadline(Boolean resetDeadline) {
        this.resetDeadline = resetDeadline;
        return this;
    }

    public UpdateWorkflowOptions withResetDeduplicationID(Boolean resetDeduplicationID) {
        this.resetDeduplicationID = resetDeduplicationID;
        return this;
    }

    public UpdateWorkflowOptions withResetStartedAtEpochMs(Boolean resetStartedAtEpochMs) {
        this.resetStartedAtEpochMs = resetStartedAtEpochMs;
        return this;
    }


    public String getOutput() {
        return output;
    }

    public String getError() {
        return error;
    }

    public Boolean getResetRecoveryAttempts() {
        return resetRecoveryAttempts;
    }

    public String getQueueName() {
        return queueName;
    }

    public Boolean getResetDeadline() {
        return resetDeadline;
    }

    public Boolean getResetDeduplicationID() {
        return resetDeduplicationID;
    }

    public Boolean getResetStartedAtEpochMs() {
        return resetStartedAtEpochMs;
    }

    public String getWhereStatus() {
        return whereStatus;
    }

    public Boolean getThrowOnFailure() {
        return throwOnFailure;
    }


    public void setOutput(String output) {
        this.output = output;
    }

    public void setError(String error) {
        this.error = error;
    }

    public void setResetRecoveryAttempts(Boolean resetRecoveryAttempts) {
        this.resetRecoveryAttempts = resetRecoveryAttempts;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void setResetDeadline(Boolean resetDeadline) {
        this.resetDeadline = resetDeadline;
    }

    public void setResetDeduplicationID(Boolean resetDeduplicationID) {
        this.resetDeduplicationID = resetDeduplicationID;
    }

    public void setResetStartedAtEpochMs(Boolean resetStartedAtEpochMs) {
        this.resetStartedAtEpochMs = resetStartedAtEpochMs;
    }

    public void setWhereStatus(String status) {
        this.whereStatus = status;
    }

    ;

    public void setThrowOnFailure(Boolean throwOnFailure) {
        this.throwOnFailure = throwOnFailure;
    }
}
