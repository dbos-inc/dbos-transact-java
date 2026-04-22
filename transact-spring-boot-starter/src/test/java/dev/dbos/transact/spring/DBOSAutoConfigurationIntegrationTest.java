package dev.dbos.transact.spring;

import static org.assertj.core.api.Assertions.assertThat;

import dev.dbos.transact.DBOS;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.sqlite.SQLiteDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class DBOSAutoConfigurationIntegrationTest {

  @Test
  void realPostgresDataSourceIsAccepted() {
    try (var pg = new PgContainer(); var ds = hikariDataSource(pg)) {
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(DBOSAutoConfiguration.class))
          .withPropertyValues("dbos.application.name=test-app")
          .withBean(DataSource.class, () -> ds)
          .run(
              context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).hasSingleBean(DBOS.class);
              });
    }
  }

  @Test
  void realPostgresDataSourceRunsMigrations() {
    try (var pg = new PgContainer(); var ds = hikariDataSource(pg)) {
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(DBOSAutoConfiguration.class))
          .withPropertyValues("dbos.application.name=test-app")
          .withBean(DataSource.class, () -> ds)
          .run(
              context -> {
                assertThat(context).hasNotFailed();
                // Verify DBOS ran migrations against the provided datasource.
                try (var conn = ds.getConnection();
                    var rs =
                        conn.getMetaData()
                            .getTables(null, "dbos", "workflow_status", new String[] {"TABLE"})) {
                  assertThat(rs.next())
                      .as("dbos.workflow_status table should exist after migration")
                      .isTrue();
                }
              });
    }
  }

  @Test
  void sqliteSpringDataSourceFails() {
    var ds = sqliteDataSource();
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(DBOSAutoConfiguration.class))
        .withPropertyValues("dbos.application.name=test-app")
        .withBean(DataSource.class, () -> ds)
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void sqliteSpringDataSourceFailsWithHelpfulMessage() {
    var ds = sqliteDataSource();
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(DBOSAutoConfiguration.class))
        .withPropertyValues("dbos.application.name=test-app")
        .withBean(DataSource.class, () -> ds)
        .run(
            context ->
                assertThat(context.getStartupFailure())
                    .hasMessageContaining("PostgreSQL")
                    .hasMessageContaining("SQLite"));
  }

  private static HikariDataSource hikariDataSource(PgContainer pg) {
    var config = new HikariConfig();
    config.setJdbcUrl(pg.jdbcUrl());
    config.setUsername(pg.username());
    config.setPassword(pg.password());
    return new HikariDataSource(config);
  }

  private static SQLiteDataSource sqliteDataSource() {
    var ds = new SQLiteDataSource();
    ds.setUrl("jdbc:sqlite::memory:");
    return ds;
  }
}
