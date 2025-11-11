package dev.dbos.transact.cli;

import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(
    name = "postgres",
    aliases = {"pg"},
    description = "Manage local Postgres database with Docker",
    mixinStandardHelpOptions = true,
    subcommands = {StartCommand.class, StopCommand.class})
public class PostgresCommand implements Runnable {

  @Override
  public void run() {
    CommandLine cmd = new CommandLine(this);
    cmd.usage(System.out);
  }

  public static String inspectContainerStatus(String containerName) throws Exception {
    var result = CommandResult.execute("docker inspect %s".formatted(containerName));
    if (result.exitCode() == 0) {
      var mapper = new ObjectMapper();
      var root = mapper.readTree(result.stdout());
      return root.get(0).get("State").get("Status").asText();
    } else {
      return null;
    }
  }
}

@Command(
    name = "start",
    description = "Start a local Postgres database Docker container",
    mixinStandardHelpOptions = true)
class StartCommand implements Callable<Integer> {

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    var out = spec.commandLine().getOut();

    if (!checkDockerInstalled()) {
      out.println("Docker not installed locally");
      return 1;
    }

    var containerName = "dbos-db";
    var imageName = "pgvector/pgvector:pg16";
    var port = 5432;
    var password = Objects.requireNonNullElse(System.getenv("PGPASSWORD"), "dbos");
    startDockerPostgres(out, containerName, imageName, password, port);

    out.format("Postgres available at postgresql://postgres:%s@localhost:%d", password, port);
    return 0;
  }

  static boolean checkDockerInstalled() throws Exception {
    var result = CommandResult.execute("docker version --format json");
    return result.exitCode() == 0;
  }

  static void startDockerPostgres(
      PrintWriter out, String containerName, String imageName, String password, int port)
      throws Exception {
    var pgData = "/var/lib/postgresql/data";

    out.println("Starting a Postgres Docker container...");

    try {
      var status = PostgresCommand.inspectContainerStatus(containerName);
      if (status.equals("running")) {
        out.format("Container %s is already running", containerName);
        return;
      }
      if (status.equals("exited")) {
        CommandResult.checkExecute("docker start %s".formatted(containerName));
        out.format("Container %s was stopped and has been restarted", containerName);
        return;
      }
    } catch (Exception e) {
      // ignore exception, proceed with creation
    }

    var queryImagesResult = CommandResult.execute("docker images -q %s".formatted(imageName));
    if (queryImagesResult.stdout().trim().isEmpty()) {
      out.format("Pulling docker image %s", imageName);
      CommandResult.checkExecute("docker pull", imageName);
    }

    var runResult =
        CommandResult.checkExecute(
            "docker run -d",
            "--name %s".formatted(containerName),
            "-e POSTGRES_PASSWORD=%s".formatted(password),
            "-e PGDATA=%s".formatted(pgData),
            "-p %d:5432".formatted(port),
            "-v %1$s:%1$s".formatted(pgData),
            "--rm",
            imageName);

    out.format("created container %s", runResult.trim());

    var url = "jdbc:postgresql://localhost:%d/postgres".formatted(port);
    var user = "postgres";
    for (var i = 0; i < 30; i++) {
      if (i % 5 == 0) {
        out.format("Waiting for Postgres Docker container to start...");
      }
      var result = checkConnectivity(url, user, password);
      if (result == null) {
        return;
      }
      Thread.sleep(1000);
    }

    var msg =
        "Failed to start Docker container: Container %s did not start in time."
            .formatted(containerName);
    throw new RuntimeException(msg);
  }

  static SQLException checkConnectivity(String url, String user, String password) {
    try (var conn = DriverManager.getConnection(url, user, password);
        var stmt = conn.createStatement()) {
      stmt.execute("SELECT 1");
      return null;
    } catch (SQLException e) {
      return e;
    }
  }
}

@Command(
    name = "stop",
    description = "Stop the local Postgres database Docker container",
    mixinStandardHelpOptions = true)
class StopCommand implements Callable<Integer> {

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    var out = spec.commandLine().getOut();

    var containerName = "dbos-db";

    out.format("Stopping Docker Postgres container %s", containerName);
    var status = PostgresCommand.inspectContainerStatus(containerName);
    if (status == null) {
      out.format("Container %s does not exist", containerName);
    } else if (status.equals("running")) {
      CommandResult.checkExecute("docker stop %s", containerName);
      out.format("Successfully stopped Docker Postgres container %s", containerName);
    } else {
      out.format("Container %s exists but is not running", containerName);
    }

    return 0;
  }
}
