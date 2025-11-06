package dev.dbos.transact.cli;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.json.JSONUtil.JsonRuntimeException;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  static final Logger logger = LoggerFactory.getLogger(DBOSCommandLine.class);

  public static void main(String[] args) {
    var exitCode = new CommandLine(new DBOSCommandLine()).execute(args);
    System.exit(exitCode);
  }

  public static String prettyPrint(Object object) {
    var mapper = new ObjectMapper();
    var writer = mapper.writerWithDefaultPrettyPrinter();
    try {
      return writer.writeValueAsString(Objects.requireNonNull(object));
    } catch (JsonProcessingException e) {
      throw new JsonRuntimeException(e);
    }
  }

  public static boolean confirm(String prompt) {
    try (var scanner = new Scanner(System.in)) {
      System.out.print(prompt);
      String input = scanner.nextLine();
      return input.equalsIgnoreCase("y") || input.equalsIgnoreCase("yes");
    }
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
        names = {"-D", "--db-url"},
        description = "Your DBOS system database URL (defaults to DBOS_SYSTEM_JDBC_URL env var)")
    String url;

    @Option(
        names = {"-U", "--db-user"},
        description = "user name for your DBOS system database (defaults to PGUSER env var)")
    String user;

    @Option(
        names = {"-P", "--db-password"},
        description = "password for your DBOS system database (defaults to PGPASSWORD env var)",
        arity = "0..1",
        interactive = true)
    String password;

    public DBOSClient createClient() {
      var url =
          Objects.requireNonNullElseGet(
              this.url, () -> System.getenv(Constants.SYSTEM_JDBC_URL_ENV_VAR));
      var user =
          Objects.requireNonNullElseGet(
              this.user, () -> System.getenv(Constants.POSTGRES_USER_ENV_VAR));
      var password =
          Objects.requireNonNullElseGet(
              this.password, () -> System.getenv(Constants.POSTGRES_PASSWORD_ENV_VAR));

      return new DBOSClient(url, user, password);
    }
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
class ResetCommand implements Callable<Integer> {

  @Option(
      names = {"-y", "--yes"},
      description = "Skip confirmation prompt")
  boolean skipConfirmation;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public Integer call() throws Exception {

    if (!skipConfirmation) {
      String prompt =
          "This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?";
      if (!DBOSCommandLine.confirm(prompt)) {
        DBOSCommandLine.logger.info("System database reset cancelled");
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
      DBOSCommandLine.logger.info(
          "System database has been reset successfully {}", pair.database());
      return 0;
    } catch (SQLException e) {
      System.err.println("Database Error {}".formatted(e.getMessage()));
      return 1;
    }
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
    })
class WorfklowCommand implements Runnable {

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("WorfklowCommand.run");
  }
}

@Command(name = "list", description = "List workflows for your application")
class WorfklowListCommand implements Runnable {

  @Option(
      names = {"-s", "--start-time"},
      description = "Retrieve workflows starting after this timestamp (ISO 8601 format)")
  String startTime;

  @Option(
      names = {"-e", "--end-time"},
      description = "Retrieve workflows starting befor this timestamp (ISO 8601 format)")
  String endTime;

  @Option(
      names = {"-S", "--status"},
      description =
          "Retrieve workflows with this status (PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)")
  String status;

  @Option(
      names = {"-n", "--name"},
      description = "Retrieve workflows with this name")
  String name;

  @Option(
      names = {"-v", "--app-version"},
      description = "Retrieve workflows with this application version")
  String appVersion;

  @Option(
      names = {"-q", "--queue"},
      description = "Retrieve workflows on this queue")
  String queue;

  // @Option(
  // names = {"-u", "--user"},
  // description = "Retrieve workflows run by this user")
  // String user;

  @Option(
      names = {"-d", "--sort-desc"},
      description = "Sort the results in descending order (older first)")
  boolean sortDescending;

  @Option(
      names = {"-l", "--limit"},
      description = "Limit the results returned",
      defaultValue = "10")
  int limit;

  @Option(
      names = {"-o", "--offset"},
      description = "Offset for pagination",
      defaultValue = "0")
  int offset;

  @Option(
      names = {"-Q", "--queues-only"},
      description = "Retrieve only queued workflows")
  boolean queuesOnly;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    var input = new ListWorkflowsInput();
    input =
        input
            .withLimit(limit)
            .withOffset(offset)
            .withSortDesc(sortDescending)
            .withQueuesOnly(queuesOnly)
            .withLoadInput(false)
            .withLoadOutput(false);
    if (status != null) {
      input = input.withAddedStatus(status);
    }
    if (name != null) {
      input = input.withWorkflowName(name);
    }
    if (appVersion != null) {
      input = input.withApplicationVersion(appVersion);
    }
    if (queue != null) {
      input = input.withQueueName(queue);
    }
    if (this.startTime != null) {
      var startTime = OffsetDateTime.parse(this.startTime);
      input = input.withStartTime(startTime);
    }
    if (this.endTime != null) {
      var endTime = OffsetDateTime.parse(this.endTime);
      input = input.withEndTime(endTime);
    }

    var client = dbOptions.createClient();
    var workflows = client.listWorkflows(input);
    var json = DBOSCommandLine.prettyPrint(workflows);
    System.out.println(json);
  }
}

@Command(name = "get", description = "Retrieve the status of a workflow")
class WorfklowGetCommand implements Runnable {

  @Parameters(index = "0")
  String workflowId;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    Objects.requireNonNull(workflowId, "workflowId parameter cannot be null");
    var input =
        new ListWorkflowsInput()
            .withAddedWorkflowId(workflowId)
            .withLoadInput(false)
            .withLoadOutput(false);
    var client = dbOptions.createClient();
    var workflows = client.listWorkflows(input);
    if (workflows.size() == 0) {
      System.err.println("Failed to retrieve workflow %s".formatted(workflowId));
    } else {
      var json = DBOSCommandLine.prettyPrint(workflows.get(0));
      System.out.println(json);
    }
  }
}

@Command(name = "steps", description = "List the steps of a workflow")
class WorfklowStepsCommand implements Runnable {

  @Parameters(index = "0")
  String workflowId;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    var client = dbOptions.createClient();
    var steps =
        client.listWorkflowSteps(
            Objects.requireNonNull(workflowId, "workflowId parameter cannot be null"));
    var json = DBOSCommandLine.prettyPrint(steps);
    System.out.println(json);
  }
}

@Command(
    name = "cancel",
    description = "Cancel a workflow so it is no longer automatically retried or restarted")
class WorfklowCancelCommand implements Runnable {

  @Parameters(index = "0")
  String workflowId;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    var client = dbOptions.createClient();
    client.cancelWorkflow(
        Objects.requireNonNull(workflowId, "workflowId parameter cannot be null"));
    DBOSCommandLine.logger.info("successfully cancelled workflow {}", workflowId);
  }
}

@Command(name = "resume", description = "Resume a workflow that has been cancelled")
class WorfklowResumeCommand implements Runnable {

  @Parameters(index = "0")
  String workflowId;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    var client = dbOptions.createClient();
    var handle =
        client.resumeWorkflow(
            Objects.requireNonNull(workflowId, "workflowId parameter cannot be null"));
    var json = DBOSCommandLine.prettyPrint(handle.getStatus());
    System.out.println(json);
  }
}

@Command(name = "fork", description = "Fork a workflow from the beginning or from a specific step")
class WorfklowForkCommand implements Runnable {

  @Parameters(index = "0", description = "Workflow ID to fork")
  String workflowId;

  @Option(
      names = {"-f", "--forked-workflow-id"},
      description = "Custom workflow ID for the forked workflow")
  String forkedWorkflowId;

  @Option(
      names = {"-v", "--application-version"},
      description = "Application version for the forked workflow")
  String appVersion;

  @Option(
      names = {"-s", "--step"},
      description = "Restart from this step [default: 1]",
      defaultValue = "1")
  Integer step;

  @Mixin DBOSCommandLine.DatabaseOptions dbOptions;

  @Override
  public void run() {
    int step = this.step == null ? 1 : this.step;
    var client = dbOptions.createClient();
    var options = new ForkOptions();
    if (forkedWorkflowId != null) {
      options = options.withForkedWorkflowId(forkedWorkflowId);
    }
    if (appVersion != null) {
      options = options.withApplicationVersion(appVersion);
    }
    var handle = client.forkWorkflow(forkedWorkflowId, step, options);
    var json = DBOSCommandLine.prettyPrint(handle.getStatus());
    System.out.println(json);
  }
}
