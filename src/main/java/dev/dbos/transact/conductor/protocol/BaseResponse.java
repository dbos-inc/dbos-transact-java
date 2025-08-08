package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BaseResponse {
    public String type;
    public String request_id;
    public String error_message;

    public BaseResponse(String type, String requestId) {
        this.type = type;
        this.request_id = requestId;
    }

    public BaseResponse(String type, String requestId, String errorMessage) {
        this.type = type;
        this.request_id = requestId;
        this.error_message = errorMessage;
    }
}
