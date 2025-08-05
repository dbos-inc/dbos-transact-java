package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.DBOSOptions;
import dev.dbos.transact.context.SetDBOSOptions;

import java.util.UUID;

public class ForkServiceImpl implements ForkService {

    private ForkService forkService;
    private DBOS dbos;

    int step1Count ;
    int step2Count ;
    int step3Count ;
    int step4Count ;
    int step5Count ;
    int child1Count ;
    int child2Count ;

    public ForkServiceImpl(DBOS d) {
        this.dbos = d;
    }

    public void setForkService(ForkService s) {
        this.forkService = s;
    }

    @Workflow(name = "worfklow")
    public String simpleWorkflow(String input) {
        forkService.stepOne("one");
        forkService.stepTwo(2);
        forkService.stepThree(2.5f);
        forkService.stepFour(Double.valueOf(23.73));
        forkService.stepFive(false);

        return input+input;
    }

    @Workflow(name = "parent")
    public String parentChild(String input) {

        forkService.stepOne("one");
        forkService.stepTwo(2);

        try(SetDBOSOptions o = new SetDBOSOptions(new DBOSOptions.Builder("child1").build())) {
            forkService.child1(25);
        }

        try(SetDBOSOptions o = new SetDBOSOptions(new DBOSOptions.Builder("child2").build())) {
            forkService.child2(25.75f);
        }

        forkService.stepFive(false);
        return input+input;
    }

    @Workflow(name = "parentasync")
    public String parentChildAsync(String input) {

        forkService.stepOne("one");
        forkService.stepTwo(2);

        WorkflowHandle<String> handle = null;
        try(SetDBOSOptions o = new SetDBOSOptions(new DBOSOptions.Builder("child1").build())) {
            handle = dbos.startWorkflow(()->forkService.child1(25));
        }

        handle.getResult();
        try(SetDBOSOptions o = new SetDBOSOptions(new DBOSOptions.Builder("child2").build())) {
            handle = dbos.startWorkflow(()->forkService.child2(25.75f));
        }

        forkService.stepFive(false);
        return input+input;
    }

    @Step(name = "one")
    public String stepOne(String input) {
        ++step1Count ;
        return input ;
    }

    @Step(name = "two")
    public int stepTwo(Integer input) {
        ++step2Count;
        return input;
    }

    @Step(name = "three")
    public float stepThree(Float input) {
        ++step3Count;
        return input;
    }

    @Step(name = "four")
    public double stepFour(Double input) {
        ++step4Count;
        return input;
    }

    @Step(name = "five")
    public void stepFive(boolean b) {
        ++step5Count;
    }

    @Workflow(name = "child1")
    public String child1(Integer number) {
        ++child1Count ;
        return String.valueOf(number);
    }

    @Workflow(name = "child2")
    public String child2(Float number) {
        ++child2Count;
        return String.valueOf(number);
    }
}
