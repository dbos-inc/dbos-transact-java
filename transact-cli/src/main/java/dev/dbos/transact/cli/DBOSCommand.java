package dev.dbos.transact.cli;

import dev.dbos.transact.DBOS;

import java.util.Objects;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;

@Command(
    name = "dbos",
    description = "DBOS CLI is a command-line interface for managing DBOS workflows",
    mixinStandardHelpOptions = true,
    subcommands = {
      MigrateCommand.class,
      PostgresCommand.class,
      ResetCommand.class,
      WorfklowCommand.class
    },
    versionProvider = DBOSCommand.class)
public class DBOSCommand implements Runnable, IVersionProvider {

  @Override
  public void run() {
    CommandLine cmd = new CommandLine(this);
    cmd.usage(System.out);
  }

  @Override
  public String[] getVersion() throws Exception {
    var pkg = DBOS.class.getPackage();
    var ver = pkg == null ? null : "v%s".formatted(pkg.getImplementationVersion());
    return new String[] {
      "${COMMAND-FULL-NAME} " + Objects.requireNonNullElse(ver, "<unknown version>")
    };
  }
}
