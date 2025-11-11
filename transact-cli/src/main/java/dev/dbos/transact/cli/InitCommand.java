package dev.dbos.transact.cli;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(
    name = "init",
    description = "Initialize a new DBOS application from a template",
    mixinStandardHelpOptions = true)
public class InitCommand implements Runnable {

  @Spec CommandSpec spec;

  @Override
  public void run() {
    var out = spec.commandLine().getOut();
    out.println("InitCommand.run");
  }
}
