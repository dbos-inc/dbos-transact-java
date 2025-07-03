package dev.dbos.transact.workflow;

public interface SimpleService {


    public void setSimpleService(SimpleService service);

    public String workWithString(String input);

    public void workWithError() throws Exception ;

    public String parentWorkflowWithoutSet(String input) ;
    public String WorkflowWithMultipleChildren(String input) throws Exception ;

    public String childWorkflow(String input) ;
    public String childWorkflow2(String input) ;
    public String childWorkflow3(String input) ;

    public String childWorkflow4(String input) throws Exception ;
    public String grandchildWorkflow(String input) ;
    public String grandParent(String input) throws Exception ;

}
