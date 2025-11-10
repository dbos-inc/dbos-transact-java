package dev.dbos.transact.cli;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public record CommandResult(int exitCode, String stdout, String stderr) {
  public static CommandResult execute(String... command) throws IOException, InterruptedException {
    var process = new ProcessBuilder(command).start();
    int exitCode = process.waitFor();

    var stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    var stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);

    return new CommandResult(exitCode, stdout, stderr);
  }

  public static String checkExecute(String... command) throws IOException, InterruptedException {
    var process = new ProcessBuilder(command).start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      var stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
      throw new RuntimeException(stderr);
    }
    var stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    return stdout;
  }
}
