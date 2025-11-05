package dev.dbos.transact.database;

public interface DisruptiveService {

  public void setSelf(DisruptiveService service);

  String dbLossBetweenSteps();

  String runChildWf();

  String wfPart1();

  String wfPart2(String id1);
}
