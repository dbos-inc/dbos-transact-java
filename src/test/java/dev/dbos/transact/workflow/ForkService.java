package dev.dbos.transact.workflow;

public interface ForkService {

    String simpleWorkflow(String input);

    String stepOne(String input) ;
    int stepTwo(Integer input) ;
    float stepThree(Float input) ;
    double stepFour(Double input ) ;
    void stepFive(boolean b) ;

    void setForkService(ForkService service) ;
}
