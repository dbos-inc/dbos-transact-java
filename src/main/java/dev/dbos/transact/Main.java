package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;

public class Main {
  public static void main(String[] args) {

    try {
      DBOSConfig dbosConfig =
          new DBOSConfig.Builder()
              .name("systemdbtest")
              .dbHost("localhost")
              .dbPort(5432)
              .dbUser("postgres")
              .sysDbName("dbos_java_sys")
              .maximumPoolSize(2)
              .runAdminServer()
              .adminServerPort(8080)
              .build();

      DBOS.initialize(dbosConfig);
      DBOS dbos = DBOS.getInstance();
      dbos.launch();

    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
