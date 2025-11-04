package dev.dbos.transact.database;

public interface DisruptiveService {

  public void setSelf(DisruptiveService service);

  String dbLossBetweenSteps();
}
