package dev.dbos.transact.config ;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.dbos.transact.Constants;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DBOSConfig {
    private final String name;
    private final String url;
    private final String dbHost;
    private final int dbPort;
    private final String dbUser ;
    private final String dbPassword ;
    private final int maximumPoolSize;
    private final int connectionTimeout;
    private final String appDbName;
    private final String sysDbName;
    private final boolean http ;
    private final int httpPort ;
    private final boolean httpAwaitOnStart ;


    private DBOSConfig(Builder builder) {
        this.name = builder.name;
        this.url = builder.url;
        this.maximumPoolSize = builder.maximumPoolSize;
        this.connectionTimeout = builder.connectionTimeout;
        this.appDbName = builder.appDbName;
        this.sysDbName = builder.sysDbName;
        this.dbUser = builder.dbUser;
        this.dbPassword = builder.dbPassword;
        this.dbHost = builder.dbHost;
        this.dbPort = builder.dbPort;
        this.http = builder.http ;
        this.httpPort = builder.httpPort ;
        this.httpAwaitOnStart = builder.httpAwaitOnStart;

    }

    public static class Builder {
        private String name;
        private String url;
        private String dbHost;
        private int dbPort;
        private String dbUser ;
        private String dbPassword ;
        private int maximumPoolSize = 3;
        private int connectionTimeout = 30000;
        private String appDbName;
        private String sysDbName;
        private boolean http = false ;
        private int httpPort ;
        private boolean httpAwaitOnStart = true ;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder dbUser(String dbUser) {
            this.dbUser = dbUser;
            return this;
        }

        public Builder dbPassword(String dbPassword) {
            this.dbPassword = dbPassword;
            return this;
        }

        public Builder maximumPoolSize(int maximumPoolSize) {
            this.maximumPoolSize = maximumPoolSize;
            return this;
        }

        public Builder connectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder appDbName(String appDbName) {
            this.appDbName = appDbName;
            return this;
        }

        public Builder dbHost(String dbHost) {
            this.dbHost = dbHost;
            return this;
        }

        public Builder dbPort(int dbPort) {
            this.dbPort = dbPort;
            return this ;
        }

        public Builder sysDbName(String sysDbName) {
            this.sysDbName = sysDbName;
            return this;
        }

        public Builder runAdminServer() {
            this.http = true ;
            return this;
        }

        public Builder adminServerPort(int port) {
            this.httpPort = port;
            return this;
        }

        public Builder adminAwaitOnStart(boolean wait) {
            this.httpAwaitOnStart = wait;
            return this;
        }

        public DBOSConfig build() {
            if (name == null) throw new IllegalArgumentException("Name is required");
            if (dbPassword == null) {
                dbPassword = System.getenv("PGPASSWORD");
            }
            return new DBOSConfig(this);
        }
    }

    // Getters
    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public String getAppDbName() {
        return appDbName;
    }

    public String getSysDbName() {
        return sysDbName;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getDbHost() {
        return dbHost;
    }

    public int getDbPort() {
        return dbPort;
    }

    public boolean isHttp() {
        return http;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public boolean isHttpAwaitOnStart() {
        return httpAwaitOnStart;
    }

    @Override
    public String toString() {
        return "DBOSConfig{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", maximumPoolSize=" + maximumPoolSize +
                ", connectionTimeout=" + connectionTimeout +
                ", appDbName='" + appDbName + '\'' +
                ", sysDbName='" + sysDbName + '\'' +
                '}';
    }


    public DataSource createDataSource(String dbName) {
        HikariConfig hikariConfig = new HikariConfig();

        String dburl = String.format("jdbc:postgresql://%s:%d/%s",dbHost,dbPort,dbName);

        System.out.println(dburl) ;

        hikariConfig.setJdbcUrl(dburl);
        hikariConfig.setUsername(dbUser);
        hikariConfig.setPassword(dbPassword);

        if (maximumPoolSize > 0) {
            hikariConfig.setMaximumPoolSize(maximumPoolSize);
        }

        if (connectionTimeout > 0) {
            hikariConfig.setConnectionTimeout(connectionTimeout);
        }

        return new HikariDataSource(hikariConfig);
    }
}
