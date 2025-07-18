package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;

public class Main {
    public static void main(String[] args) {

        System.out.println("Hello DBOS Transact!");

        try {
            DBOSConfig dbosConfig = new DBOSConfig
                    .Builder()
                    .name("systemdbtest")
                    .dbHost("localhost")
                    .dbPort(5432)
                    .dbUser("postgres")
                    .sysDbName("dbos_java_sys")
                    .maximumPoolSize(2)
                    .http()
                    .httpPort(3000)
                    .build();

            DBOS.initialize(dbosConfig);
            DBOS dbos = DBOS.getInstance();
            dbos.launch();

        } catch(Exception e) {
            System.out.println(e);
        }


    }
}