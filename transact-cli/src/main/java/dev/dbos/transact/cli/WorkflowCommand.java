package dev.dbos.transact.cli;

import dev.dbos.transact.json.JSONUtil.JsonRuntimeException;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.OffsetDateTime;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(
    name = "workflow",
    aliases = {"wf"},
    description = "Manage DBOS workflows",
    subcommands = {
      ListCommand.class,
      GetCommand.class,
      StepsCommand.class,
      CancelCommand.class,
      ResumeCommand.class,
      ForkCommand.class,
    })
public class WorkflowCommand implements Runnable {

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  boolean help;

  @Override
  public void run() {
    CommandLine cmd = new CommandLine(this);
    cmd.usage(System.out);
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
}

@Command(name = "list", description = "List workflows for your application")
class ListCommand implements Runnable {

  @Option(
      names = {"-s", "--start-time"},
      description = "Retrieve workflows starting after this timestamp (ISO 8601 format)")
  String startTime;

  @Option(
      names = {"-e", "--end-time"},
      description = "Retrieve workflows starting before this timestamp (ISO 8601 format)")
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

  @Mixin DatabaseOptions dbOptions;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  boolean help;

  @Spec CommandSpec spec;

  @Override
  public void run() {
    var out = spec.commandLine().getOut();

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
    var json = WorkflowCommand.prettyPrint(workflows);
    out.println(json);
  }
}

@Command(name = "get", description = "Retrieve the status of a workflow")
class GetCommand implements Runnable {

  @Parameters(index = "0", description = "Workflow ID to retrieve")
  String workflowId;

  @Mixin DatabaseOptions dbOptions;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  boolean help;

  @Spec CommandSpec spec;

  @Override
  public void run() {
    var out = spec.commandLine().getOut();

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
      var json = WorkflowCommand.prettyPrint(workflows.get(0));
      out.println(json);
    }
  }
}

@Command(name = "steps", description = "List the steps of a workflow")
class StepsCommand implements Runnable {

  @Parameters(index = "0", description = "Workflow ID to list steps for")
  String workflowId;

  @Mixin DatabaseOptions dbOptions;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  boolean help;

  @Spec CommandSpec spec;

  @Override
  public void run() {
    var out = spec.commandLine().getOut();

    var client = dbOptions.createClient();
    var steps =
        client.listWorkflowSteps(
            Objects.requireNonNull(workflowId, "workflowId parameter cannot be null"));
    var json = WorkflowCommand.prettyPrint(steps);
    out.println(json);
  }
}

@Command(
    name = "cancel",
    description = "Cancel a workflow so it is no longer automatically retried or restarted")
class CancelCommand implements Runnable {

  @Parameters(index = "0", description = "Workflow ID to cancel")
  String workflowId;

  @Mixin DatabaseOptions dbOptions;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  boolean help;

  @Spec CommandSpec spec;

  @Override
  public void run() {
    var out = spec.commandLine().getOut();

    var client = dbOptions.createClient();
    client.cancelWorkflow(
        Objects.requireNonNull(workflowId, "workflowId parameter cannot be null"));
    out.format("Successfully cancelled workflow %s\n", workflowId);
  }
}

@Command(name = "resume", description = "Resume a workflow that has been cancelled")
class ResumeCommand implements Runnable {

  @Parameters(index = "0", description = "Workflow ID to resume")
  String workflowId;

  @Mixin DatabaseOptions dbOptions;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  boolean help;

  @Spec CommandSpec spec;

  @Override
  public void run() {
    var out = spec.commandLine().getOut();

    var client = dbOptions.createClient();
    var handle =
        client.resumeWorkflow(
            Objects.requireNonNull(workflowId, "workflowId parameter cannot be null"));
    var json = WorkflowCommand.prettyPrint(handle.getStatus());
    out.println(json);
  }
}

@Command(name = "fork", description = "Fork a workflow from the beginning or from a specific step")
class ForkCommand implements Runnable {

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
      description = "Restart from this step [default: ${DEFAULT-VALUE}]",
      defaultValue = "1")
  Integer step;

  @Mixin DatabaseOptions dbOptions;

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Display this help message")
  boolean help;

  @Spec CommandSpec spec;

  @Override
  public void run() {
    var out = spec.commandLine().getOut();

    int step = this.step == null ? 1 : this.step;
    var client = dbOptions.createClient();
    var options = new ForkOptions();
    if (forkedWorkflowId != null) {
      options = options.withForkedWorkflowId(forkedWorkflowId);
    }
    if (appVersion != null) {
      options = options.withApplicationVersion(appVersion);
    }
    var handle = client.forkWorkflow(workflowId, step, options);
    var json = WorkflowCommand.prettyPrint(handle.getStatus());
    out.println(json);
  }
}
