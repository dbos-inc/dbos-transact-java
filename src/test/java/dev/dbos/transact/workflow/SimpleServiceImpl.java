package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.context.SetWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.queue.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleServiceImpl implements SimpleService {

    Logger logger = LoggerFactory.getLogger(SimpleServiceImpl.class);

    private SimpleService simpleService;

    public static int executionCount = 0;

    @Workflow(name = "workWithString")
    public String workWithString(String input) {
        logger.info("Executed workflow workWithString");
        SimpleServiceImpl.executionCount++;
        return "Processed: " + input;
    }

    @Workflow(name = "workError")
    public void workWithError() throws Exception {
        throw new Exception("DBOS Test error");
    }

    @Workflow(name = "parentWorkflowWithoutSet")
    public String parentWorkflowWithoutSet(String input) {
        String result = input;

        result = result + simpleService.childWorkflow("abc");

        return result;
    }

    @Workflow(name = "childWorkflow")
    public String childWorkflow(String input) {
        return input;
    }

    public void setSimpleService(SimpleService service) {
        this.simpleService = service;
    }

    @Workflow(name = "WorkflowWithMultipleChildren")
    public String WorkflowWithMultipleChildren(String input) throws Exception {
        var dbos = DBOSContext.dbosInstance().get();
        String result = input;

        try (SetWorkflowID id = new SetWorkflowID("child1")) {
            simpleService.childWorkflow("abc");
        }
        result = result + dbos.retrieveWorkflow("child1").getResult();

        try (SetWorkflowID id = new SetWorkflowID("child2")) {
            simpleService.childWorkflow2("def");
        }
        result = result + dbos.retrieveWorkflow("child2").getResult();

        try (SetWorkflowID id = new SetWorkflowID("child3")) {
            simpleService.childWorkflow3("ghi");
        }
        result = result + dbos.retrieveWorkflow("child3").getResult();

        return result;
    }

    @Workflow(name = "childWorkflow2")
    public String childWorkflow2(String input) {
        return input;
    }

    @Workflow(name = "childWorkflow3")
    public String childWorkflow3(String input) {
        return input;
    }

    @Workflow(name = "childWorkflow4")
    public String childWorkflow4(String input) throws Exception {
        String result = input;
        try (SetWorkflowID id = new SetWorkflowID("child5")) {
            simpleService.grandchildWorkflow(input);
        }
        result = "c-" + DBOSContext.dbosInstance().get().retrieveWorkflow("child5").getResult();
        return result;
    }

    @Workflow(name = "grandchildWorkflow")
    public String grandchildWorkflow(String input) {
        return "gc-" + input;
    }

    @Workflow(name = "grandParent")
    public String grandParent(String input) throws Exception {
        String result = input;
        try (SetWorkflowID id = new SetWorkflowID("child4")) {
            simpleService.childWorkflow4(input);
        }
        result = "p-" + DBOSContext.dbosInstance().get().retrieveWorkflow("child4").getResult();
        return result;
    }

    @Workflow(name = "syncWithQueued")
    public String syncWithQueued() {

        System.out.println("In syncWithQueued " + DBOSContextHolder.get().getWorkflowId());

        // TODO: when we require WF/Q registration to happen before launch, this will break
        Queue q = DBOSContext.dbosInstance().get().Queue("childQ").build();
        for (int i = 0; i < 3; i++) {

            String wid = "child" + i;
            WorkflowOptions options = new WorkflowOptions.Builder(wid).queue(q).build();
            try (SetWorkflowOptions o = new SetWorkflowOptions(options)) {
                simpleService.childWorkflow(wid);
            }
        }

        return "QueuedChildren";
    }

    @Workflow(name = "longWorkflow")
    public String longWorkflow(String input) {

        simpleService.stepWithSleep(1);
        simpleService.stepWithSleep(1);

        logger.info("Done with longWorkflow");
        return input + input;
    }

    @Step(name = "stepWithSleep")
    public void stepWithSleep(long sleepSeconds) {

        try {
            logger.info("Step sleeping for " + sleepSeconds);
            Thread.sleep(sleepSeconds * 1000);
        } catch (Exception e) {
            logger.error("Sleep interrupted", e);
        }
    }

    @Workflow(name = "childWorkflowWithSleep")
    public String childWorkflowWithSleep(String input, long sleepSeconds)
            throws InterruptedException {
        logger.info("Child sleeping for " + sleepSeconds);
        Thread.sleep(sleepSeconds * 1000);
        logger.info("Child done sleeping for " + sleepSeconds);
        return input;
    }

    @Workflow(name = "longParent")
    public String longParent(String input, long sleepSeconds, long timeoutSeconds)
            throws InterruptedException {

        logger.info("In longParent");
        String workflowId = "childwf";
        WorkflowOptions options = new WorkflowOptions.Builder(workflowId).timeout(timeoutSeconds).build();

        WorkflowHandle<String> handle = null;
        try (SetWorkflowOptions o = new SetWorkflowOptions(options)) {
            handle = DBOSContext.dbosInstance().get()
                    .startWorkflow(() -> simpleService.childWorkflowWithSleep(input, sleepSeconds));
        }

        String result = handle.getResult();

        logger.info("Done with longWorkflow");
        return input + result;
    }
}
