package dev.dbos.transact.cli;

import picocli.CommandLine.Command;

@Command(name = "init")
public class InitCommand implements Runnable {

  @Override
  public void run() {
    System.out.println("InitCommand.run");
  }
}
