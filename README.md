<div align="center">

# DBOS Transact: Lightweight Durable Workflows

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc) &nbsp;&nbsp;•&nbsp;&nbsp; [Discord](https://discord.com/invite/jsmC6pXGgX)
</div>

---

## What is DBOS?

DBOS provides lightweight durable workflows built on top of Postgres.
Instead of managing your own workflow orchestrator or task queue system, you can use DBOS to add durable workflows and queues to your program in just a few lines of code.

To get started, follow the [quickstart](https://docs.dbos.dev/quickstart) to install this open-source library and connect it to a Postgres database.
Then, annotate workflows and steps in your program to make it durable!
That's all you need to do&mdash;DBOS is entirely contained in this open-source library, there's no additional infrastructure for you to configure or manage.

## When Should I Use DBOS?

You should consider using DBOS if your application needs to **reliably handle failures**.
For example, you might be building a payments service that must reliably process transactions even if servers crash mid-operation, or a long-running data pipeline that needs to resume seamlessly from checkpoints rather than restart from the beginning when interrupted.

Handling failures is costly and complicated, requiring complex state management and recovery logic as well as heavyweight tools like external orchestration services.
DBOS makes it simpler: annotate your code to checkpoint it in Postgres and automatically recover from any failure.
DBOS also provides powerful Postgres-backed primitives that makes it easier to write and operate reliable code, including durable queues, notifications, scheduling, event processing, and programmatic workflow management.

## Features

<details open><summary><strong>💾 Durable Workflows</strong></summary>

####

DBOS workflows make your program **durable** by checkpointing its state in Postgres.
If your program ever fails, when it restarts all your workflows will automatically resume from the last completed step.

You add durable workflows to your existing Java program by annotating ordinary functions as workflows and steps:

```java

public interface SimpleWorkflowService {

    void setSimpleWorkflowService(SimpleWorkflowService e);
    String exampleWorkflow(String input) ;
    void stepOne() ;
    void stepTwo() ;
}

public class SimpleWorkflowServiceImpl implements SimpleWorkflowService {

    public void setSimpleWorkflowService(SimpleWorkflowService simpleWorkflow) {
        this.simpleWorkflowService = simpleWorkflow;
    }
    
    @Workflow(name = "exampleWorkflow")
    public String exampleWorkflow(String input) {
        return input + input;
    }
    @Step(name = "stepOne")
    public void stepOne() {
        logger.info("Executed stepOne") ;
    }
    @Step(name = "stepTwo")
    public void stepTwo() {
        logger.info("Executed stepTwo") ;
    }
    
}

public class Demo {
    
    public static void main(String[] args) {
        
        DBOSConfig dbosConfig = new DBOSConfig.Builder()
                .name("demo")
                .dbHost("localhost")
                .dbPort(5432)
                .dbUser("postgres")
                .sysDbName("demo_dbos_sys")
                .build() ;

        DBOS.initialize(dbosConfig);
        DBOS dbos = DBOS.getInstance();
        dbos.launch();
        
        SimpleWorkflowService syncExample = dbos.<SimpleWorkflowService>Workflow()
                .interfaceClass(SimpleWorkflowService.class)
                .implementation(new SimpleWorkflowServiceImpl())
                .build();
        syncExample.setSimpleWorkflowService(syncExample);

        String output = syncExample.exampleWorkflow("HelloDBOS") ;
        System.out.println("Sync result: " + output);
    }
}


```

Workflows are particularly useful for

- Orchestrating business processes so they seamlessly recover from any failure.
- Building observable and fault-tolerant data pipelines.
- Operating an AI agent, or any application that relies on unreliable or non-deterministic APIs.

[Read more ↗️]()

</details>






