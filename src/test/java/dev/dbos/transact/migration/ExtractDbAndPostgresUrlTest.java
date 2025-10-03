package dev.dbos.transact.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import dev.dbos.transact.migrations.MigrationManager;

public class ExtractDbAndPostgresUrlTest {

    @Test
    public void extractDbAndPostgresUrl() {
        var originalUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys?user=alice&ssl=true";
        var pair = MigrationManager.extractDbAndPostgresUrl(originalUrl);

        assertEquals("dbos_java_sys", pair.database());
        assertEquals("jdbc:postgresql://localhost:5432/postgres?user=alice&ssl=true", pair.url());
    }
}
