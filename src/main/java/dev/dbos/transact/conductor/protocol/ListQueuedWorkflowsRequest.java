package dev.dbos.transact.conductor.protocol;

public class ListQueuedWorkflowsRequest extends BaseMessage {
    Body body;

    public static class Body {
        public String workflowName;
        public String startTime;
        public String endTime;
        public String status;
        public String queueName;
        public Integer limit;
        public Integer offset;
        public boolean sortDesc;
        public Boolean loadInput;
    }
}
