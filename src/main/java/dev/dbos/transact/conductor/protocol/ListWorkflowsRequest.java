package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.OffsetDateTime;
import java.util.List;

public class ListWorkflowsRequest extends BaseMessage {
    public Body body;

    public static class Body {
        public List<String> workflow_uuids;
        public String workflow_name;
        public String authenticated_user;
        public String start_time;
        public String end_time;
        public String status;
        public String application_version;
        public Integer limit;
        public Integer offset;
        public boolean sort_desc;
        public Boolean load_input;
        public Boolean load_output;
    }

    public ListWorkflowsInput getInput() {
        return new ListWorkflowsInput(
                body.workflow_uuids,
                body.workflow_name,
                body.authenticated_user,
                body.start_time != null ? OffsetDateTime.parse(body.start_time) : null,
                body.end_time != null ? OffsetDateTime.parse(body.end_time) : null,
                body.status,
                body.application_version,
                body.limit,
                body.offset,
                body.sort_desc,
                null);
    }
}
