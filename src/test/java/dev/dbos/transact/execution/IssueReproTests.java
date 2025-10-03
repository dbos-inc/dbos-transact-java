package dev.dbos.transact.execution;

import org.junit.jupiter.api.Test;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;

public class IssueReproTests {
    @Test
    public void issue108() throws Exception {
        var dbosConfig = new DBOSConfig.Builder()
                .appName("systemdbtest")
                .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
                .dbUser("postgres")
                .build();
        var dbos = DBOS.initialize(dbosConfig);
        try {
            var proxy = dbos.<Issue108Service>Workflow().interfaceClass(Issue108Service.class)
                    .implementation(new Issue108ServiceImpl(dbos)).build();
            dbos.launch();
            proxy.workflow();
        } finally {
            dbos.shutdown();
        }
    }
}
