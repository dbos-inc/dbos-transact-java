package dev.dbos.transact.cli;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;
import picocli.CommandLine.Command;

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

  @Override
  public Integer call() throws Exception {
    if (!checkDockerInstalled()) {
      System.out.println("Docker not installed locally");
      return 1;
    }

    var containerName = "dbos-db";
    var imageName = "pgvector/pgvector:pg16";
    var port = 5432;
    var password = Objects.requireNonNullElse(System.getenv("PGPASSWORD"), "dbos");
    startDockerPostgres(containerName, imageName, password, port);

    var msg =
        "Postgres available at postgresql://postgres:%s@localhost:%d".formatted(password, port);
    System.out.println(msg);
    return 0;
  }

  static boolean checkDockerInstalled() throws Exception {
    var result = CommandResult.execute("docker version --format json");
    return result.exitCode() == 0;
  }

  static void startDockerPostgres(String containerName, String imageName, String password, int port)
      throws Exception {
    var pgData = "/var/lib/postgresql/data";

    System.out.println("Starting a Postgres Docker container...");

    try {
      var status = PostgresCommand.inspectContainerStatus(containerName);
      if (status.equals("running")) {
        System.out.println("Container %s is already running".formatted(containerName));
        return;
      }
      if (status.equals("exited")) {
        CommandResult.checkExecute("docker start %s".formatted(containerName));
        System.out.println(
            "Container %s was stopped and has been restarted".formatted(containerName));
        return;
      }
    } catch (Exception e) {
      // ignore exception, proceed with creation
    }

    var queryImagesResult = CommandResult.execute("docker images -q %s".formatted(imageName));
    if (queryImagesResult.stdout().trim().isEmpty()) {
      System.out.println("Pulling docker image %s".formatted(imageName));
      CommandResult.checkExecute("docker pull %s".formatted(imageName));
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

    System.out.println("created container %s".formatted(runResult.trim()));

    var url = "jdbc:postgresql://localhost:%d/postgres".formatted(port);
    var user = "postgres";
    for (var i = 0; i < 30; i++) {
      if (i % 5 == 0) {
        System.out.println("Waiting for Postgres Docker container to start...");
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

  @Override
  public Integer call() throws Exception {
    var containerName = "dbos-db";

    System.out.println("Stopping Docker Postgres container %s".formatted(containerName));
    var status = PostgresCommand.inspectContainerStatus(containerName);
    if (status == null) {
      System.out.println("Container %s does not exist".formatted(containerName));
    } else if (status.equals("running")) {
      CommandResult.checkExecute("docker stop %s".formatted(containerName));
      System.out.println(
          "Successfully stopped Docker Postgres container %s".formatted(containerName));
    } else {
      System.out.println("Container %s exists but is not running".formatted(containerName));
    }

    return 0;
  }
}
