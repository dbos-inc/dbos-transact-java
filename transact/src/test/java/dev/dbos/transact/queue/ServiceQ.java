package dev.dbos.transact.queue;

public interface ServiceQ {

  String simpleQWorkflow(String input);

  Double limitWorkflow(String var1, String var2);

  String priorityWorkflow(int input);
}
