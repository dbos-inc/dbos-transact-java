package dev.dbos.transact.utlils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.dbos.transact.config.DBOSConfig;

import javax.sql.DataSource;

public class DBUtils {

    public static DataSource createDataSource(DBOSConfig config) {

        HikariConfig hikariConfig = new HikariConfig();

        String dburl = String.format("jdbc:postgresql://%s:%d/%s",config.getDbHost(),config.getDbPort(), config.getSysDbName());

        hikariConfig.setJdbcUrl(dburl);
        hikariConfig.setUsername(config.getDbUser());
        hikariConfig.setPassword(config.getDbPassword());

        int maximumPoolSize = config.getMaximumPoolSize();
        if (maximumPoolSize > 0) {
            hikariConfig.setMaximumPoolSize(maximumPoolSize);
        } else {
            hikariConfig.setMaximumPoolSize(2);
        }

        int connectionTimeout = config.getConnectionTimeout();
        if (connectionTimeout > 0) {
            hikariConfig.setConnectionTimeout(connectionTimeout);
        }

       return new HikariDataSource(hikariConfig);
    }

    public void closeDS(HikariDataSource ds) {
        ds.close();
    }
}
