package dev.dbos.transact.devhawk;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.workflow.Workflow;

public class HawkServiceImpl implements HawkService {
    private HawkService proxy;

    public void setProxy(HawkService proxy) {
        this.proxy = proxy;
    }

    @Workflow
    @Override
    public String simpleWorkflow(String workflowId) {
        if (workflowId != null) {
            var ctxWfId = DBOSContext.workflowId().get();
            if (workflowId != ctxWfId) {
                throw new RuntimeException("workflow ID mismatch %s %s".formatted(workflowId, ctxWfId));
            }
        }
        return LocalDate.now().format(DateTimeFormatter.ISO_DATE);
    }

    @Workflow
    @Override
    public String recvWorkflow() {
        var dbos = DBOSContext.dbosInstance().get();
        dbos.recv(null, 60);
        return LocalDate.now().format(DateTimeFormatter.ISO_DATE);
    }


}
