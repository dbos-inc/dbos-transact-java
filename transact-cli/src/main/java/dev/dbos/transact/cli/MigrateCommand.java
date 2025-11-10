package dev.dbos.transact.cli;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "migrate")
public class MigrateCommand implements Runnable {

  @Mixin DatabaseOptions dbOptions;

  @Override
  public void run() {
    System.out.println("MigrateCommand.run");
  }
}
