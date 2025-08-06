package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.context.DBOSOptions;
import dev.dbos.transact.context.SetDBOSOptions;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.queue.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SimpleServiceImpl implements SimpleService {

    Logger logger = LoggerFactory.getLogger(SimpleServiceImpl.class);

    private SimpleService simpleService;

    public static int executionCount = 0 ;

    private DBOS dbos;

    public SimpleServiceImpl() {

    }

    public SimpleServiceImpl(DBOS d) {
        dbos = d;
    }



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

    @Workflow(name = "WorkflowWithMultipleChildren")
    public String WorkflowWithMultipleChildren(String input) throws Exception {
        String result = input;

        try (SetWorkflowID id = new SetWorkflowID("child1")) {
            simpleService.childWorkflow("abc");
        }
        result = result + DBOS.retrieveWorkflow("child1").getResult() ;

        try (SetWorkflowID id = new SetWorkflowID("child2")) {
            simpleService.childWorkflow2("def");
        }
        result = result + DBOS.retrieveWorkflow("child2").getResult() ;

        try (SetWorkflowID id = new SetWorkflowID("child3")) {
            simpleService.childWorkflow3("ghi");
        }
        result = result + DBOS.retrieveWorkflow("child3").getResult() ;

        return result ;
    }

    @Workflow(name = "childWorkflow2")
    public String childWorkflow2(String input) {
        return input ;
    }

    @Workflow(name = "childWorkflow3")
    public String childWorkflow3(String input) {
        return input ;
    }

    @Workflow(name = "childWorkflow4")
    public String childWorkflow4(String input) throws Exception{
        String result = input ;
        try (SetWorkflowID id = new SetWorkflowID("child5")) {
            simpleService.grandchildWorkflow(input);
        }
        result = "c-" + DBOS.retrieveWorkflow("child5").getResult() ;
        return result ;
    }

    @Workflow(name = "grandchildWorkflow")
    public String grandchildWorkflow(String input) {
        return "gc-"+input ;
    }

    @Workflow(name = "grandParent")
    public String grandParent(String input) throws Exception{
        String result = input ;
        try (SetWorkflowID id = new SetWorkflowID("child4")) {
            simpleService.childWorkflow4(input);
        }
        result = "p-" + DBOS.retrieveWorkflow("child4").getResult() ;
        return result ;
    }

    @Workflow(name = "syncWithQueued")
    public String syncWithQueued() {

        System.out.println("In syncWithQueued " + DBOSContextHolder.get().getWorkflowId()) ;

        Queue q= new DBOS.QueueBuilder("childQ").build();
        for (int i = 0 ; i < 3 ; i++) {

            String wid = "child"+i;
            DBOSOptions options = new DBOSOptions.Builder(wid).queue(q).build();
            try (SetDBOSOptions o = new SetDBOSOptions(options)) {
                simpleService.childWorkflow(wid);
            }
        }

        return "QueuedChildren" ;
    }

    @Workflow(name = "longWorkflow")
    public String longWorkflow(String input) {

        simpleService.stepWithSleep(1);
        simpleService.stepWithSleep(1);

        logger.info("Done with longWorkflow");
        return input+input ;

    }

    @Step(name = "stepWithSleep")
    public void stepWithSleep(long sleepSeconds)  {

        try {
            logger.info("Step sleeping for " + sleepSeconds) ;
            Thread.sleep(sleepSeconds * 1000);
        } catch(Exception e) {
            logger.error("Sleep interrupted", e) ;
        }
    }

    @Workflow(name = "childWorkflowWithSleep")
    public String childWorkflowWithSleep(String input, long sleepSeconds) throws InterruptedException {
        logger.info("Child sleeping for " + sleepSeconds) ;
        Thread.sleep(sleepSeconds*1000);
        logger.info("Child done sleeping for " + sleepSeconds) ;
        return input ;
    }

    @Workflow(name = "longParent")
    public String longParent(String input, long sleepSeconds, long timeoutSeconds) throws InterruptedException {

        logger.info("In longParent") ;
        String workflowId = "childwf" ;
        DBOSOptions options = new DBOSOptions.Builder(workflowId).timeout(timeoutSeconds).build();

        WorkflowHandle<String> handle = null ;
        try (SetDBOSOptions o = new SetDBOSOptions(options)) {
            handle = dbos.startWorkflow(()->simpleService.childWorkflowWithSleep(input, sleepSeconds));
        }

        String result = handle.getResult();

        logger.info("Done with longWorkflow");
        return input+result;

    }

}
