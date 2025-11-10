package dev.dbos.transact.cli;

import picocli.CommandLine.Command;

@Command(name = "init", description = "Initialize a new DBOS application from a template")
public class InitCommand implements Runnable {

  @Override
  public void run() {
    System.out.println("InitCommand.run");
  }
}
