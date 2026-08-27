package dev.dbos.transact.cli;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.database.dao.ApplicationRenameDAO;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(
    name = "rename-application",
    description = "Re-own a system database's rows after an application is renamed")
public class RenameApplicationCommand implements Callable<Integer> {

  @Option(
      names = {"-y", "--yes"},
      description = "Skip confirmation prompt")
  boolean skipConfirmation;

  @Option(
      names = {"-f", "--from"},
      description = "The application's previous name. Omit to only adopt unclaimed rows.")
  String from;

  @Option(
      names = {"-t", "--to"},
      required = true,
      description = "The application that ends up owning the rows")
  String to;

  @Option(
      names = {"--adopt-unclaimed-rows"},
      description = "Also take rows no application owns")
  boolean adoptUnclaimedRows;

  @Option(
      names = {"--batch-size"},
      description = "Workflows and steps re-owned per transaction [default: ${DEFAULT-VALUE}]")
  int batchSize = ApplicationRenameDAO.DEFAULT_RENAME_BATCH_SIZE;

  @Mixin DatabaseOptions dbOptions;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  boolean help;

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    var out = spec.commandLine().getOut();
    var err = spec.commandLine().getErr();

    var sources = new ArrayList<String>();
    if (from != null && !from.isEmpty()) {
      sources.add("'%s's rows".formatted(from));
    }
    if (adoptUnclaimedRows) {
      sources.add("rows no application owns");
    }
    if (sources.isEmpty()) {
      err.println("Nothing to re-own: pass --from, --adopt-unclaimed-rows, or both.");
      return 1;
    }
    // Reject here rather than let it reach SQL after the first transaction has committed.
    if (batchSize < 1) {
      err.format("Invalid --batch-size '%d': expected a positive integer.%n", batchSize);
      return 1;
    }

    if (!skipConfirmation) {
      var prompt =
          ("This command re-owns %s in your DBOS system database as '%s'. Stop the application"
                  + " being renamed before running this. Are you sure you want to proceed?%n")
              .formatted(String.join(" and ", sources), to);
      if (!ResetCommand.confirm(prompt)) {
        out.println("Application rename cancelled");
        return 0;
      }
    }

    try (var client =
        new DBOSClient(
            dbOptions.url(), dbOptions.user(), dbOptions.password(), dbOptions.schema())) {
      var moved = client.renameApplication(from, to, batchSize, adoptUnclaimedRows);
      out.format(
          "Re-owned as '%s': %d workflow(s), %d step(s), %d queue(s), %d schedule(s), %d"
              + " version(s)%n",
          to,
          moved.workflows(),
          moved.steps(),
          moved.queues(),
          moved.schedules(),
          moved.versions());
      return 0;
    } catch (IllegalArgumentException e) {
      err.println(e.getMessage());
      return 1;
    }
  }
}
