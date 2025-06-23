package dev.dbos.transact.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class WorkflowImpl {

    Logger logger = LoggerFactory.getLogger(WorkflowImpl.class);

    public static int executionCount = 0 ;

    @Workflow(name = "workWithString")
    public String workWithString(String input) {
        logger.info("Executed workflow workWithString");
        WorkflowImpl.executionCount++;
        return "Processed: " + input ;
    }

    @Workflow(name = "workError")
    public void workWithError() throws Exception {
       throw new Exception("DBOS Test error") ;
    }




}
