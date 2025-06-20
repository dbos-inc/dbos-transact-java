package dev.dbos.transact.exceptions;

public enum ErrorCode {

    UNEXPECTED(1),
    WORKFLOW_CONFLICT(2),
    QUEUE_DUPLICATED(3) ,
    DEAD_LETTER_QUEUE(4) ,
    NONEXISTENT_WORKFLOW(5) ;

    private int code ;

    ErrorCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code ;
    }
}
