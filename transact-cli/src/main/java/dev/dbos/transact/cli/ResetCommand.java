package dev.dbos.transact.cli;

import dev.dbos.transact.migrations.MigrationManager;

import java.sql.DriverManager;
import java.util.concurrent.Callable;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

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

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    var out = spec.commandLine().getOut();

    if (!skipConfirmation) {
      String prompt =
          "This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?";
      if (!DBOSCommand.confirm(prompt)) {
        out.println("System database reset cancelled");
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
      out.format("System database %s has been reset successfully", pair.database());
      return 0;
    }
  }
}
