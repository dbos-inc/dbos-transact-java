package dev.dbos.transact.conductor.protocol;

public class SuccessResponse extends BaseResponse {
    public boolean success;

    public SuccessResponse() {
    }

    public SuccessResponse(BaseMessage message, boolean success) {
        super(message.type, message.request_id);
        this.success = success;
    }

    public SuccessResponse(BaseMessage message, Exception ex) {
        super(message.type, message.request_id, ex.getMessage());
    }
}
