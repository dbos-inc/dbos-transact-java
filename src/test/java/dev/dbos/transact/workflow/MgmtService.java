package dev.dbos.transact.workflow;

import java.util.concurrent.CountDownLatch;

public interface MgmtService {

    void setMgmtService(MgmtService m);
    int simpleWorkflow(int input) ;
    void stepOne() ;
    void stepTwo() ;
    void stepThree() ;

    int getStepsExecuted() ;


}
