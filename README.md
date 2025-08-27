<div align="center">

# DBOS Transact : Lightweight Durable Workflows
# Java version in preview + under active development

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

public interface WorkflowService {
    void setStepService(StepService stepService);
    String exampleWorkflow(String input) ;
}

public interface StepService {
    void stepOne() ;
    void stepTwo() ;
}

public class WorkflowServiceImpl implements WorkflowService {

    private Stepservice stepService;
    public void setStepService(StepService stepService) {
        this.stepService = stepService;
    }
    
    @Workflow(name = "exampleWorkflow")
    public String exampleWorkflow(String input) {
        stepService.stepOne();
        stepService.stepTwo();
        return input + input;
    }
    
}

public class StepServiceImpl implements StepService {

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
        // Remember to export the DB password to the env variable PGPASSWORD

        DBOS dbos = DBOS.initialize(dbosConfig);
        dbos.launch();
        
        WorkflowService syncExample = dbos.<WorkflowService>Workflow()
                .interfaceClass(WorkflowService.class)
                .implementation(new WorkflowServiceImpl())
                .build();

        StepService steps = dbos.<StepService>Workflow()
                .interfaceClass(StepService.class)
                .implementation(new StepServiceImpl())
                .build();
        
        syncExample.setStepService(steps);

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

<details><summary><strong>📒 Durable Queues</strong></summary>

####

DBOS queues help you **durably** run tasks in the background.
You can enqueue a task (which can be a single step or an entire workflow) from a durable workflow and one of your processes will pick it up for execution.
DBOS manages the execution of your tasks: it guarantees that tasks complete, and that their callers get their results without needing to resubmit them, even if your application is interrupted.

Queues also provide flow control, so you can limit the concurrency of your tasks on a per-queue or per-process basis.
You can also set timeouts for tasks, rate limit how often queued tasks are executed, deduplicate tasks, or prioritize tasks.

You can add queues to your workflows in just a couple lines of code.
They don't require a separate queueing service or message broker&mdash;just Postgres.

```java


 public void queuedTasks() {
     Queue q = dbos.Queue("childQ").build();

     for (int i = 0; i < 3; i++) {

         String wid = "child" + i;
         DBOSOptions options = new DBOSOptions.Builder(wid).queue(q).build();
         List<WorkflowHandle<String>> handles = new ArrayList<>();
         try (SetDBOSOptions o = new SetDBOSOptions(options)) {
             handles.add(dbos.startWorkflow(()->simpleService.childWorkflow(wid)));
         }
     }

     for (int i = 0 ; i < 3 ; i++) {
         String wid = "child"+i;
         System.out.println(h.getResult());
     }
 }
```

[Read more ↗️](https://docs.dbos.dev/python/tutorials/queue-tutorial)

</details>

<details><summary><strong>📒 Asynchronous execution </strong></summary>

####

DBOS can run your workflows asynchronously without you needing to make any changes to the interface or implementation. 

This is ideal for long-running workflows whose result might not be available immediately. 
You code can return at a later point and check the status for completion and/or retrive the result.


```java


 public void runAsyncWorkflow() {

     WorkflowService asyncExample = dbos.<WorkflowService>Workflow()
             .interfaceClass(WorkflowService.class)
             .implementation(new WorkflowServiceImpl())
             .build();

     StepService steps = dbos.<StepService>Workflow()
             .interfaceClass(StepService.class)
             .implementation(new StepServiceImpl())
             .build();

     syncExample.setStepService(steps);

     String workflowId = "wf-124";
     options = new DBOSOptions.Builder(workflowId).async().build();
     WorkflowHandle<String> handle = null;
     try (SetDBOSOptions id = new SetDBOSOptions(options)) {
         handle = dbos.startWorkflow(()->syncExample.exampleWorkflow("HelloDBOS"));
     }
     
     result = handle.getResult();
 }
```

[Read more ↗️](https://docs.dbos.dev/python/tutorials/queue-tutorial)

</details>


<details><summary><strong>📅 Durable Scheduling</strong></summary>

####

Schedule workflows using cron syntax, or use durable sleep to pause workflows for as long as you like (even days or weeks) before executing.

You can schedule a workflow using a single annotation:

```java

public class SchedulerImpl {
    
    @Workflow(name = "every5Second")
    @Scheduled(cron = "0/5 * * * * ?")
    public void every5Second(Instant schedule , Instant actual) {
        log.info("Executed workflow  "+  schedule.toString() + "   " + actual.toString()) ;
    }
}

// In your main
// dbos.scheduleWorkflow(new SchedulerImpl());
```

[Read more ↗️](https://docs.dbos.dev/python/tutorials/scheduled-workflows)

</details>

<details><summary><strong>📫 Durable Notifications</strong></summary>

####

Pause your workflow executions until a notification is received, or emit events from your workflow to send progress updates to external clients.
All notifications are stored in Postgres, so they can be sent and received with exactly-once semantics.
Set durable timeouts when waiting for events, so you can wait for as long as you like (even days or weeks) through interruptions or restarts, then resume once a notification arrives or the timeout is reached.

For example, build a reliable billing workflow that durably waits for a notification from a payments service, processing it exactly-once:

```java
@workflow(name = "billing")
public void billingWorkflow() {
    // retrieve DBOS instance from context
    var dbos = DBOSContext.dbosInstance().get();

    // Calculate the charge, then submit the bill to a payments service
    String payment_status = (String) dbos.recv(PAYMENT_STATUS, timeout = payment_service_timeout);
    if (payment_status.equals("paid")) {
        // handle paid
    } else {
        // handle not paid
    }
}

@workflow(name = "payment") 
public void payment() {
    var dbos = DBOSContext.dbosInstance().get();
    dbos.send(targetWorkflowId, PAYMENT_STATUS, "paid") ;
}
      
```
</details>


## Getting Started

To get started, follow the [quickstart](https://docs.dbos.dev/quickstart) to install this open-source library and connect it to a Postgres database.
Then, check out the [programming guide](https://docs.dbos.dev/python/programming-guide) to learn how to build with durable workflows and queues.

## Documentation

[https://docs.dbos.dev](https://docs.dbos.dev)

## Examples

[https://docs.dbos.dev/examples](https://docs.dbos.dev/examples)

## DBOS vs. Other Systems

<details><summary><strong>DBOS vs. Temporal</strong></summary>

####

Both DBOS and Temporal provide durable execution, but DBOS is implemented in a lightweight Postgres-backed library whereas Temporal is implemented in an externally orchestrated server.

You can add DBOS to your program by installing this open-source library, connecting it to Postgres, and annotating workflows and steps.
By contrast, to add Temporal to your program, you must rearchitect your program to move your workflows and steps (activities) to a Temporal worker, configure a Temporal server to orchestrate those workflows, and access your workflows only through a Temporal client.
[This blog post](https://www.dbos.dev/blog/durable-execution-coding-comparison) makes the comparison in more detail.

**When to use DBOS:** You need to add durable workflows to your applications with minimal rearchitecting, or you are using Postgres.

**When to use Temporal:** You don't want to add Postgres to your stack, or you need a language DBOS doesn't support yet.

</details>

<details><summary><strong>DBOS vs. Airflow</strong></summary>

####

DBOS and Airflow both provide workflow abstractions.
Airflow is targeted at data science use cases, providing many out-of-the-box connectors but requiring workflows be written as explicit DAGs and externally orchestrating them from an Airflow cluster.
Airflow is designed for batch operations and does not provide good performance for streaming or real-time use cases.
DBOS is general-purpose, but is often used for data pipelines, allowing developers to write workflows as code and requiring no infrastructure except Postgres.

**When to use DBOS:** You need the flexibility of writing workflows as code, or you need higher performance than Airflow is capable of (particularly for streaming or real-time use cases).

**When to use Airflow:** You need Airflow's ecosystem of connectors.

</details>

<details><summary><strong>DBOS vs. Celery/BullMQ</strong></summary>

####

DBOS provides a similar queue abstraction to dedicated queueing systems like Celery or BullMQ: you can declare queues, submit tasks to them, and control their flow with concurrency limits, rate limits, timeouts, prioritization, etc.
However, DBOS queues are **durable and Postgres-backed** and integrate with durable workflows.
For example, in DBOS you can write a durable workflow that enqueues a thousand tasks and waits for their results.
DBOS checkpoints the workflow and each of its tasks in Postgres, guaranteeing that even if failures or interruptions occur, the tasks will complete and the workflow will collect their results.
By contrast, Celery/BullMQ are Redis-backed and don't provide workflows, so they provide fewer guarantees but better performance.

**When to use DBOS:** You need the reliability of enqueueing tasks from durable workflows.

**When to use Celery/BullMQ**: You don't need durability, or you need very high throughput beyond what your Postgres server can support.
</details>

## Community

If you want to ask questions or hang out with the community, join us on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).






