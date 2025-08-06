package dev.dbos.transact.step;

import dev.dbos.transact.workflow.Step;

public class ServiceBImpl implements ServiceB {

    @Step(name = "step1")
    public String step1(String input) {
        return input;
    }

    @Step(name = "step2")
    public String step2(String input) {
        return input;
    }

    @Step(name = "step3")
    public String step3(String input, boolean throwError) throws Exception {
        if (throwError) {
            throw new Exception("step3 error");
        }

        return input;
    }

    @Step(name = "step4")
    public String step4(String input) {
        return input;
    }

    @Step(name = "step5")
    public String step5(String input) {
        return input;
    }
}
