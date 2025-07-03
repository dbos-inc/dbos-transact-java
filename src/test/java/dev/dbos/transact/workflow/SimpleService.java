package dev.dbos.transact.workflow;

public interface SimpleService {


    public void setSimpleService(SimpleService service);

    public String workWithString(String input);

    public void workWithError() throws Exception ;

    public String parentWorkflowWithoutSet(String input) ;

    public String childWorkflow(String input) ;

}
