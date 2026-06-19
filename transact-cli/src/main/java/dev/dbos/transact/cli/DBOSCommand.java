package dev.dbos.transact.cli;

import dev.dbos.transact.DBOS;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;

@Command(
    name = "dbos",
    description = "DBOS CLI is a command-line interface for managing the DBOS system database",
    mixinStandardHelpOptions = true,
    subcommands = {MigrateCommand.class, ResetCommand.class},
    versionProvider = DBOSCommand.class)
public class DBOSCommand implements Runnable, IVersionProvider {

  @Override
  public void run() {
    CommandLine cmd = new CommandLine(this);
    cmd.usage(System.out);
  }

  @Override
  public String[] getVersion() throws Exception {
    return new String[] {
      "${COMMAND-FULL-NAME} "
          + (DBOS.version() == null ? "<unknown version>" : "v" + DBOS.version())
    };
  }
}
