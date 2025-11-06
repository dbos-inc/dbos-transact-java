package dev.dbos.transact.cli;

import dev.dbos.transact.DBOS;

import java.util.Objects;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
    name = "dbos",
    mixinStandardHelpOptions = true,
    subcommands = {
      InitCommand.class,
      MigrateCommand.class,
      ResetCommand.class,
      WorfklowCommand.class
    },
    versionProvider = DBOSCommandLine.class)
public class DBOSCommandLine implements Runnable, IVersionProvider {

  public static void main(String[] args) {
    var exitCode = new CommandLine(new DBOSCommandLine()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public void run() {
    System.out.println("Hello, World!");
  }

  @Override
  public String[] getVersion() throws Exception {
    var pkg = DBOS.class.getPackage();
    var ver = pkg == null ? null : "v%s".formatted(pkg.getImplementationVersion());
    return new String[] {
      "${COMMAND-FULL-NAME} " + Objects.requireNonNullElse(ver, "<unknown version>")
    };
  }

  public static class DatabaseOptions {
    @Option(
        names = {"-s", "--sys-db-url"},
        description = "DBOS system database URL")
    String sysDbUrl;

    @Option(
        names = {"-u", "--user"},
        description = "DBOS system database URL")
    String sysDbUser;

    @Option(
        names = {"-p", "--password"},
        description = "Passphrase",
        arity = "0..1",
        interactive = true)
    String sysDbPassword;
  }
}

@Command(name = "init")
class InitCommand implements Runnable {

  @Override
  public void run() {
    System.out.println("InitCommand.run");
  }
}

@Command(name = "migrate")
class MigrateCommand implements Runnable {

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("MigrateCommand.run");
  }
}

@Command(name = "reset")
class ResetCommand implements Runnable {

  @Option(
      names = {"-y", "--yes"},
      description = "Skip confirmation prompt")
  boolean skipConfirmation;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println(
        "ResetCommand.run %s %s %s"
            .formatted(dbOptions.sysDbUrl, dbOptions.sysDbUser, dbOptions.sysDbPassword));
  }
}

@Command(
    name = "worfklow",
    subcommands = {
      WorfklowListCommand.class,
      WorfklowGetCommand.class,
      WorfklowStepsCommand.class,
      WorfklowCancelCommand.class,
      WorfklowResumeCommand.class,
      WorfklowForkCommand.class,
      WorfklowQueueCommand.class
    })
class WorfklowCommand implements Runnable {

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("WorfklowCommand.run");
  }
}

@Command(name = "list")
class WorfklowListCommand implements Runnable {

  @Parameters(index = "0")
  String workflowId;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("WorfklowListCommand.run");
  }
}

@Command(name = "get")
class WorfklowGetCommand implements Runnable {

  @Parameters(index = "0")
  String workflowId;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("WorfklowGetCommand.run");
  }
}

@Command(name = "steps")
class WorfklowStepsCommand implements Runnable {

  @Parameters(index = "0")
  String workflowId;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("WorfklowStepsCommand.run");
  }
}

@Command(name = "cancel")
class WorfklowCancelCommand implements Runnable {

  @Parameters(index = "0")
  String workflowId;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("WorfklowCancelCommand.run");
  }
}

@Command(name = "resume")
class WorfklowResumeCommand implements Runnable {

  @Parameters(index = "0")
  String workflowId;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("WorfklowResumeCommand.run");
  }
}

@Command(name = "fork")
class WorfklowForkCommand implements Runnable {

  @Parameters(index = "0")
  String workflowId;

  @Option(
      names = {"-f", "--forked-workflow-id"},
      description = "Workflow ID for the forked workflow")
  String forkedWorkflowId;

  @Option(
      names = {"-v", "--application-version"},
      description = "application version for the forked workflow")
  String appVersion;

  @Option(
      names = {"-S", "--step"},
      description = "Restart from this step [default: 1]",
      defaultValue = "1")
  Integer step;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("WorfklowForkCommand.run");
  }
}

@Command(
    name = "queue",
    subcommands = {WorfklowQueueListCommand.class})
class WorfklowQueueCommand implements Runnable {

  @Override
  public void run() {
    System.out.println("WorfklowQueueCommand.run");
  }
}

@Command(name = "list")
class WorfklowQueueListCommand implements Runnable {

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("WorfklowQueueListCommand.run");
  }
}
