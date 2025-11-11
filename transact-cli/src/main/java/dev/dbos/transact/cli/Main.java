package dev.dbos.transact.cli;

import picocli.CommandLine;

public class Main {
  public static void main(String[] args) {
    var cmd = new CommandLine(new DBOSCommand());
    var exitCode = cmd.execute(args);
    System.exit(exitCode);
  }
}
