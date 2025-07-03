package dev.dbos.transact.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SimpleServiceImpl implements SimpleService {

    Logger logger = LoggerFactory.getLogger(SimpleServiceImpl.class);

    private SimpleService simpleService;

    public static int executionCount = 0 ;

    @Workflow(name = "workWithString")
    public String workWithString(String input) {
        logger.info("Executed workflow workWithString");
        SimpleServiceImpl.executionCount++;
        return "Processed: " + input ;
    }

    @Workflow(name = "workError")
    public void workWithError() throws Exception {
       throw new Exception("DBOS Test error") ;
    }

    @Workflow(name = "parentWorkflowWithoutSet")
    public String parentWorkflowWithoutSet(String input) {
        String result = input;

        result = result + simpleService.childWorkflow("abc");

        return result;
    }

    @Workflow(name = "childWorkflow")
    public String childWorkflow(String input) {
        return input ;
    }

    public void setSimpleService(SimpleService service) {
        this.simpleService = service ;
    }

}
