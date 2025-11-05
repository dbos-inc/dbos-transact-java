package dev.dbos.transact.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "dbos", subcommands = {InitCommand.class, MigrateCommand.class, ResetCommand.class, WorfklowCommand.class})
public class DBOSCommandLine implements Runnable {
    
    public static void main(String[] args) {
        var exitCode = new CommandLine(new DBOSCommandLine()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        System.out.println("Hello, World!");
    }
}

@Command(name="init")
class InitCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("InitCommand.run");
    }

}

@Command(name="migrate")
class MigrateCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("MigrateCommand.run");
    }

}

@Command(name="reset")
class ResetCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("ResetCommand.run");
    }

}

@Command(name="worfklow", subcommands = {WorfklowListCommand.class, WorfklowGetCommand.class, WorfklowStepsCommand.class, WorfklowCancelCommand.class, WorfklowResumeCommand.class, WorfklowForkCommand.class, WorfklowQueueCommand.class})
class WorfklowCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("WorfklowCommand.run");
    }
}

@Command(name="list")
class WorfklowListCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("WorfklowListCommand.run");
    }
}

@Command(name="get")
class WorfklowGetCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("WorfklowGetCommand.run");
    }
}


@Command(name="steps")
class WorfklowStepsCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("WorfklowStepsCommand.run");
    }
}


@Command(name="cancel")
class WorfklowCancelCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("WorfklowCancelCommand.run");
    }
}


@Command(name="resume")
class WorfklowResumeCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("WorfklowResumeCommand.run");
    }
}


@Command(name="fork")
class WorfklowForkCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("WorfklowForkCommand.run");
    }
}


@Command(name="queue", subcommands = {WorfklowQueueListCommand.class})
class WorfklowQueueCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("WorfklowQueueCommand.run");
    }
}

@Command(name="list")
class WorfklowQueueListCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("WorfklowQueueListCommand.run");
    }
}