package dev.dbos.transact.workflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class MgmtServiceImpl implements MgmtService{

    Logger logger = LoggerFactory.getLogger(MgmtServiceImpl.class) ;

    private volatile int stepsExecuted ;
    CountDownLatch mainThreadEvent ;
    CountDownLatch workflowEvent ;

    MgmtService service ;

    public MgmtServiceImpl(CountDownLatch mainLatch, CountDownLatch workLatch) {
        this.mainThreadEvent = mainLatch ;
        this.workflowEvent = workLatch ;

    }

    public void setMgmtService(MgmtService m) {
        service = m;
    }


    @Workflow(name = "myworkflow")
    public int simpleWorkflow(int input) {

        try {
            service.stepOne();
            mainThreadEvent.countDown();
            workflowEvent.await();
            service.stepTwo();
            service.stepThree();

        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }

        return input;
    }

    @Step(name = "one")
    public synchronized void stepOne() {
        ++stepsExecuted;
    }

    @Step(name = "two")
    public synchronized void stepTwo() {
        ++stepsExecuted;
    }

    @Step(name = "three")
    public synchronized void stepThree() {
        ++stepsExecuted;
    }

    public synchronized int getStepsExecuted() {
        return stepsExecuted ;
    }

}
