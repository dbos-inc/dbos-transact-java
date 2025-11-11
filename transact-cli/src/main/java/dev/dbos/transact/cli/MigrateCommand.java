package dev.dbos.transact.cli;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "migrate",
    description = "Create DBOS system tables",
    mixinStandardHelpOptions = true)
public class MigrateCommand implements Runnable {

  @ArgGroup(heading = "System Database Options:%n")
  DatabaseOptions dbOptions;

  @Option(
      names = {"-r", "--app-role"},
      description = "The role with which you will run your DBOS application")
  String appRole;

  @Override
  public void run() {
    System.out.println("MigrateCommand.run");
  }
}
