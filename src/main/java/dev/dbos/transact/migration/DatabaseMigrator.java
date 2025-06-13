package dev.dbos.transact.migration;

import dev.dbos.transact.config.DatabaseConfig;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class DatabaseMigrator {
    public static void runMigrations() {

        Logger logger = LoggerFactory.getLogger(DatabaseMigrator.class) ;

        DataSource dataSource = DatabaseConfig.createDataSource();
        String schema = DatabaseConfig.getSchema();

        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .schemas(schema)
                .defaultSchema(schema)
                .locations("classpath:db/migration")
                .load();

        flyway.migrate();
        logger.info("Database migrations completed successfully");
    }
}
