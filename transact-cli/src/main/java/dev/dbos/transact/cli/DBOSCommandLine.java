package dev.dbos.transact.cli;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.json.JSONUtil.JsonRuntimeException;

import java.util.Objects;
import java.util.Scanner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;

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

  public static void main(String[] args) {
    var exitCode = new CommandLine(new DBOSCommandLine()).execute(args);
    System.exit(exitCode);
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
}
