package dev.dbos.transact.execution;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.StepOptions;

public class Issue108ServiceImpl implements Issue108Service {

    private final DBOS dbos;

    public Issue108ServiceImpl(DBOS dbos) {
        this.dbos = dbos;
    }
    
    // purposefully leaving @Workflow off
    @Override
    public void workflow() {
        dbos.runStep(() -> stepOne(), new StepOptions("stepOne"));
        dbos.runStep(() -> stepTwo(), new StepOptions("stepTwo"));
    }

    private void stepOne() {
        System.out.println("Step one completed!");
    }

    private void stepTwo() {
        System.out.println("Step two completed!");
    }
}
