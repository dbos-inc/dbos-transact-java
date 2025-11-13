<div align="center">

# DBOS Transact: Lightweight Durable Workflows

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp;  [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc) &nbsp;&nbsp;•&nbsp;&nbsp; [Discord](https://discord.com/invite/jsmC6pXGgX)
</div>

---

## What is DBOS?

DBOS provides lightweight durable workflows built on top of Postgres.
Essentially, it helps you write long-lived, reliable code that can survive crashes, restarts, and failures without losing state or duplicating work.

As your workflows run, DBOS checkpoints each step they take in a Postgres database.
When a process stops (fails, intentionally suspends, or a machine dies), your program can recover from those checkpoints to restore its exact state and continue from where it left off, as if nothing happened.

In practice, this makes it easier to build reliable systems for use cases like AI agents, data synchronization, payments, or anything that takes minutes, days, or weeks to complete.
Rather than bolting on ad-hoc retry logic and database checkpoints, DBOS workflows give you one consistent model for ensuring your programs can recover from any failure from exactly where they left off.

This library contains all you need to add durable workflows to your program: there's no separate service or orchestrator or any external dependencies except Postgres.
Because it's just a library, you can incrementally add it to your projects, and it works out of the box with frameworks like Spring.
And because it's built on Postgres, it natively supports all the tooling you're familiar with (backups, GUIs, CLI tools) and works with any Postgres provider.

## Features

<details open><summary><strong>💾 Durable Workflows</strong></summary>

####

Workflows make your program **durable** by checkpointing its state in Postgres.
If your program ever fails, when it restarts all your workflows will automatically resume from the last completed step.

You add durable workflows to your existing Java program in just a few lines of code by registering ordinary functions as workflows and steps:

```java

interface Example {
    public void workflow();
}

class ExampleImpl implements Example {

    private void stepOne() {
        System.out.println("Step one completed!");
    }

    private void stepTwo() {
        System.out.println("Step two completed!");
    }

    @Workflow()
    public void workflow() {
        DBOS.runStep(() -> stepOne(), "stepOne");
        DBOS.runStep(() -> stepTwo(), "stepTwo");
    }
}
```

Workflows are particularly useful for

- Orchestrating business processes so they seamlessly recover from any failure.
- Building observable and fault-tolerant data pipelines.
- Operating an AI agent, or any application that relies on unreliable or non-deterministic APIs.

[Read more ↗️]()

</details>

<details><summary><strong>📒 Asynchronous execution </strong></summary>

####

You can run your workflows asynchronously without making any changes to their interface or implementation. 

This is ideal for long-running background workflows: you code can return at a later point and check the status for completion and/or retrieve the result.


```java
var handle = DBOS.startWorkflow(()->example.exampleWorkflow("HelloDBOS"));
result = handle.getResult();
```

[Read more ↗️](https://docs.dbos.dev/java/tutorials/workflow-tutorial#starting-workflows-in-the-background)

</details>

<details><summary><strong>📒 Durable Queues</strong></summary>

####

DBOS queues help you **durably** run tasks in the background.
You can enqueue a task from a durable workflow and one of your processes will pick it up for execution.
DBOS manages the execution of your tasks: it guarantees that tasks complete, and that their callers get their results without needing to resubmit them, even if your application is interrupted.

Queues also provide flow control, so you can limit the concurrency of your tasks on a per-queue or per-process basis.
You can also set timeouts for tasks, rate limit how often queued tasks are executed, deduplicate tasks, or prioritize tasks.

You can add queues to your workflows in just a couple lines of code.
They don't require a separate queueing service or message broker&mdash;just Postgres.

```java
 public void queuedTasks() {
    for (int i = 0; i < 3; i++) {
        String workflowId = "child" + i;
        var options = new StartWorkflowOptions(workflowId).withQueue(q);
        List<WorkflowHandle<String>> handles = new ArrayList<>();
        handles.add(DBOS.startWorkflow(()->simpleService.childWorkflow(workflowId), options));
    }

    for (int i = 0 ; i < 3 ; i++) {
        String workflowId = "child"+i;
        var h = DBOS.retrieveWorkflow(workflowId);
        System.out.println(h.getResult());
    }
}

// In your main
var queue = new Queue("exampleQueue");
DBOS.registerQueue(queue);
```

[Read more ↗️](https://docs.dbos.dev/java/tutorials/queue-tutorial)

</details>


<details><summary><strong>📅 Durable Scheduling</strong></summary>

####

Schedule workflows using cron syntax, or use durable sleep to pause workflows for as long as you like (even days or weeks) before executing.

You can schedule a workflow using a single annotation:

```java

public class SchedulerImpl implements Scheduler {
    
    @Workflow(name = "every5Second")
    @Scheduled(cron = "0/5 * * * * ?")
    public void every5Second(Instant schedule , Instant actual) {
        log.info("Executed workflow  "+  schedule.toString() + "   " + actual.toString()) ;
    }
}

// In your main
DBOS.registerWorkflows(Scheduler.class, new SchedulerImpl());
```

[Read more ↗️](https://docs.dbos.dev/java/tutorials/scheduled-workflows)

</details>

<details><summary><strong>📫 Durable Notifications</strong></summary>

####

Pause your workflow executions until a notification is received, or emit events from your workflow to send progress updates to external clients.
All notifications are stored in Postgres, so they can be sent and received with exactly-once semantics.
Set durable timeouts when waiting for events, so you can wait for as long as you like (even days or weeks) through interruptions or restarts, then resume once a notification arrives or the timeout is reached.

For example, build a reliable billing workflow that durably waits for a notification from a payments service, processing it exactly-once:

```java
@Workflow(name = "billing")
public void billingWorkflow() {
    // Calculate the charge, then submit the bill to a payments service
    String paymentStatus = (String) DBOS.recv(PAYMENT_STATUS, paymentServiceTimeout);
    if (paymentStatus.equals("paid")) {
        // handle paid
    } else {
        // handle not paid
    }
}

@Workflow(name = "payment") 
public void payment(String targetWorkflowId) {
    DBOS.send(targetWorkflowId, PAYMENT_STATUS, "paid") ;
}
      
```
</details>


## Getting Started

To get started, follow the [quickstart](https://docs.dbos.dev/quickstart?language=java) to install this open-source library and connect it to a Postgres database.
Then, check out the [programming guide](https://docs.dbos.dev/java/programming-guide) to learn how to build with durable workflows and queues.

## Documentation

[https://docs.dbos.dev](https://docs.dbos.dev)

## Examples

[https://docs.dbos.dev/examples](https://docs.dbos.dev/examples)

## DBOS vs. Other Systems

<details><summary><strong>DBOS vs. Temporal</strong></summary>

####

Both DBOS and Temporal provide durable execution, but DBOS is implemented in a lightweight Postgres-backed library whereas Temporal is implemented in an externally orchestrated server.

You can add DBOS to your program by installing the open-source library, connecting it to Postgres, and annotating workflows and steps.
By contrast, to add Temporal to your program, you must rearchitect your program to move your workflows and steps (activities) to a Temporal worker, configure a Temporal server to orchestrate those workflows, and access your workflows only through a Temporal client.
[This page](https://docs.dbos.dev/explanations/comparing-temporal) makes the comparison in more detail.

</details>

## Community

If you want to ask questions or hang out with the community, join us on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).






