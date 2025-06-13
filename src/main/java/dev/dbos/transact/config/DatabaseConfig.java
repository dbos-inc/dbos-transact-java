package dev.dbos.transact.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class DatabaseConfig {
    private static final Config config = ConfigFactory.load().getConfig("database");

    public static DataSource createDataSource() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getString("url"));
        hikariConfig.setUsername(config.getString("user"));
        hikariConfig.setPassword(config.getString("password"));

        // Optional pool settings
        if (config.hasPath("pool")) {
            hikariConfig.setMaximumPoolSize(config.getInt("pool.maximumPoolSize"));
            hikariConfig.setConnectionTimeout(config.getInt("pool.connectionTimeout"));
        }

        return new HikariDataSource(hikariConfig);
    }

    public static String getSchema() {
        return config.getString("schema");
    }
}