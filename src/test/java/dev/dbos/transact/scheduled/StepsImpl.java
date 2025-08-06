package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Step;

public class StepsImpl implements Steps {

    @Step(name = "stepOne")
    public void stepOne() {
    }

    @Step(name = "stepTwo")
    public void stepTwo() {
    }
}
