package dev.dbos.transact.cli;

import dev.dbos.transact.migrations.MigrationManager;

import java.sql.DriverManager;
import java.util.concurrent.Callable;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "reset",
    description = "Reset the DBOS system database",
    mixinStandardHelpOptions = true)
public class ResetCommand implements Callable<Integer> {

  @Option(
      names = {"-y", "--yes"},
      description = "Skip confirmation prompt")
  boolean skipConfirmation;

  @ArgGroup(heading = "System Database Options:%n")
  DatabaseOptions dbOptions;

  @Override
  public Integer call() throws Exception {

    if (!skipConfirmation) {
      String prompt =
          "This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?";
      if (!DBOSCommand.confirm(prompt)) {
        System.out.println("System database reset cancelled");
        return 0;
      }
    }

    var pair = MigrationManager.extractDbAndPostgresUrl(dbOptions.url);
    var dropDbSql = String.format("DROP DATABASE IF EXISTS %s WITH (FORCE)", pair.database());
    var createDbSql = String.format("CREATE DATABASE %s", pair.database());
    try (var conn = DriverManager.getConnection(pair.url(), dbOptions.user, dbOptions.password);
        var stmt = conn.createStatement()) {
      stmt.execute(dropDbSql);
      stmt.execute(createDbSql);
      System.out.println(
          "System database has been reset successfully %s".formatted(pair.database()));
      return 0;
    }
  }
}
