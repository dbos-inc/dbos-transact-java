package dev.dbos.transact.migration;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.config.DatabaseConfig;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class DatabaseMigrator {

    public static void runMigrations(DBOSConfig dbconfig) {

        Logger logger = LoggerFactory.getLogger(DatabaseMigrator.class) ;

        if (dbconfig == null) {
            logger.warn("No database configuration. Skipping migration");
            return ;
        }

        DataSource dataSource = dbconfig.createDataSource();
        String schema = "dbos";

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
