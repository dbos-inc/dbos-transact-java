package dev.dbos.transact.exceptions;

public enum ErrorCode {

    WORKFLOW_CONFLICT(1),
    QUEUE_DUPLICATED(2) ,
    DEAD_LETTER_QUEUE(3) ;

    private int code ;

    ErrorCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code ;
    }
}
