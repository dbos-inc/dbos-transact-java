package dev.dbos.transact.exceptions;

public enum ErrorCode {
    UNEXPECTED(1), WORKFLOW_CONFLICT(2), QUEUE_DUPLICATED(3), DEAD_LETTER_QUEUE(
            4), NONEXISTENT_WORKFLOW(5), AWAITED_WORKFLOW_CANCEL(6), WORKFLOW_CANCELLED(
                    7), UNEXPECTED_STEP(8), WORKFLOW_FUNCTION_NOT_FOUND(9), SLEEP_NOT_IN_WORKFLOW(
                            10), RESUME_WORKFLOW_ERROR(11), FORK_WORKFLOW_ERROR(12);

    private int code;

    ErrorCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
