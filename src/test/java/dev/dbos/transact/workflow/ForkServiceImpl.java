package dev.dbos.transact.workflow;

public class ForkServiceImpl implements ForkService {

    private ForkService forkService;

    int step1Count ;
    int step2Count ;
    int step3Count ;
    int step4Count ;
    int step5Count ;

    public ForkServiceImpl() {

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
}
