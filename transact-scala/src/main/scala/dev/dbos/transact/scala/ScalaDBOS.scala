package dev.dbos.transact.scala

import dev.dbos.transact.{DBOS => JDBOS, StartWorkflowOptions}
import dev.dbos.transact.config.DBOSConfig
import dev.dbos.transact.execution.ThrowingSupplier
import dev.dbos.transact.internal.DBOSIntegration
import dev.dbos.transact.workflow.{
  Debouncer,
  ForkOptions,
  ListWorkflowsInput,
  Queue,
  ScheduleStatus,
  SerializationStrategy,
  StepInfo,
  StepOptions,
  VersionInfo,
  WorkflowHandle,
  WorkflowSchedule,
  WorkflowStatus,
}

import java.lang.reflect.Method
import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters.*

type WorkflowRunner[T] = String => T

class ScalaDBOS private (private val dbos: JDBOS) extends AutoCloseable {

  // ── Lifecycle ───────────────────────────────────────────────

  def launch(): Unit = dbos.launch()

  def shutdown(): Unit = dbos.shutdown()

  override def close(): Unit = dbos.close()

  // ── Config / Integration ────────────────────────────────────

  def integration: DBOSIntegration = dbos.integration()

  def config: DBOSConfig = integration.config()

  // ── Steps ───────────────────────────────────────────────────

  def runStep[T](name: String)(f: => T): T =
    dbos.runStep(
      new ThrowingSupplier[T, Exception] {
        def execute(): T = f
      },
      name
    )

  def runStep[T](opts: StepOptions)(f: => T): T =
    dbos.runStep(
      new ThrowingSupplier[T, Exception] {
        def execute(): T = f
      },
      opts
    )

  // ── Starting Workflows ──────────────────────────────────────

  def startWorkflow[T](
      f: => T
  ): WorkflowHandle[T, Exception] =
    dbos.startWorkflow(
      new ThrowingSupplier[T, Exception] {
        def execute(): T = f
      }
    )

  def startWorkflow[T](
      f: => T,
      opts: StartWorkflowOptions
  ): WorkflowHandle[T, Exception] =
    dbos.startWorkflow(
      new ThrowingSupplier[T, Exception] {
        def execute(): T = f
      },
      opts
    )

  // ── Workflow Registration (reflection-based, like Clojure) ─

  /**
    * Register a Scala function as a named DBOS workflow using reflection.
    * The function receives `this ScalaDBOS` as its argument so it can call
    * runStep / setEvent etc.
    *
    * Returns a `WorkflowRunner[T]` — a function from taskId to result.
    */
  def registerWorkflow[T](
      name: String
  )(f: ScalaDBOS => T): WorkflowRunner[T] = {
    val applyMethod: Method =
      classOf[Function[_, _]].getMethod("apply", classOf[Object])
    val className: String = f.getClass.getName
    val registered = integration.registerWorkflow(
      name,
      className,
      null,
      f.asInstanceOf[AnyRef],
      applyMethod,
      null,
      null
    )
    (taskId: String) => {
      val handle = integration.startRegisteredWorkflow(
        registered,
        Array(this),
        new StartWorkflowOptions(taskId)
      )
      handle.getResult.asInstanceOf[T]
    }
  }

  /**
    * Register a no-arg Scala function as a named DBOS workflow.
    * The function captures its dependencies (e.g. `this`) in a closure.
    */
  def registerWorkflow0[T](name: String)(f: () => T): WorkflowRunner[T] = {
    val applyMethod: Method =
      classOf[Function0[_]].getMethod("apply")
    val className: String = f.getClass.getName
    val registered = integration.registerWorkflow(
      name,
      className,
      null,
      f.asInstanceOf[AnyRef],
      applyMethod,
      null,
      null
    )
    (taskId: String) => {
      val handle = integration.startRegisteredWorkflow(
        registered,
        Array(),
        new StartWorkflowOptions(taskId)
      )
      handle.getResult.asInstanceOf[T]
    }
  }

  // ── Workflow Query / Management ─────────────────────────────

  def getResult[T](workflowId: String): T =
    dbos.getResult(workflowId)

  def getWorkflowStatus(workflowId: String): Option[WorkflowStatus] =
    Option(dbos.getWorkflowStatus(workflowId).orElse(null))

  def retrieveWorkflow[T, E <: Exception](
      workflowId: String
  ): WorkflowHandle[T, E] =
    dbos.retrieveWorkflow(workflowId)

  def cancelWorkflow(workflowId: String): Unit =
    dbos.cancelWorkflow(workflowId)

  def cancelWorkflows(workflowIds: Seq[String]): Unit =
    dbos.cancelWorkflows(workflowIds.asJava)

  def deleteWorkflow(workflowId: String): Unit =
    dbos.deleteWorkflow(workflowId)

  def deleteWorkflow(workflowId: String, deleteChildren: Boolean): Unit =
    dbos.deleteWorkflow(workflowId, deleteChildren)

  def deleteWorkflows(workflowIds: Seq[String]): Unit =
    dbos.deleteWorkflows(workflowIds.asJava)

  def deleteWorkflows(workflowIds: Seq[String], deleteChildren: Boolean): Unit =
    dbos.deleteWorkflows(workflowIds.asJava, deleteChildren)

  def resumeWorkflow[T, E <: Exception](
      workflowId: String,
      queueName: String = null
  ): WorkflowHandle[T, E] =
    dbos.resumeWorkflow(workflowId, queueName)

  def resumeWorkflows(
      workflowIds: Seq[String],
      queueName: String = null
  ): Seq[WorkflowHandle[Object, Exception]] =
    dbos.resumeWorkflows(workflowIds.asJava, queueName).asScala.toSeq

  def forkWorkflow[T, E <: Exception](
      workflowId: String,
      startStep: Int,
      options: ForkOptions = new ForkOptions()
  ): WorkflowHandle[T, E] =
    dbos.forkWorkflow(workflowId, startStep, options)

  def listWorkflows(
      input: ListWorkflowsInput = null
  ): Seq[WorkflowStatus] =
    dbos.listWorkflows(input).asScala.toSeq

  def listWorkflowSteps(
      workflowId: String,
      limit: Integer = null,
      offset: Integer = null
  ): Seq[StepInfo] =
    dbos.listWorkflowSteps(workflowId, limit, offset).asScala.toSeq

  // ── Events ──────────────────────────────────────────────────

  def setEvent(key: String, value: Any): Unit =
    dbos.setEvent(key, value)

  def setEvent(
      key: String,
      value: Any,
      serialization: SerializationStrategy
  ): Unit =
    dbos.setEvent(key, value, serialization)

  def getEvent[T](
      workflowId: String,
      key: String,
      timeout: Duration
  ): Option[T] =
    Option(dbos.getEvent(workflowId, key, timeout).orElse(null))
      .map(_.asInstanceOf[T])

  def getAllEvents(workflowId: String): Map[String, Any] =
    dbos.getAllEvents(workflowId).asScala.toMap

  // ── Messages ────────────────────────────────────────────────

  def send(
      destinationId: String,
      message: Any,
      topic: String = null,
      idempotencyKey: String = null
  ): Unit =
    dbos.send(destinationId, message, topic, idempotencyKey)

  def recv[T](topic: String, timeout: Duration): Option[T] =
    Option(dbos.recv(topic, timeout).orElse(null)).map(_.asInstanceOf[T])

  // ── Queues ──────────────────────────────────────────────────

  def registerQueue(queue: Queue): Unit =
    dbos.registerQueue(queue)

  def registerQueues(queues: Queue*): Unit =
    dbos.registerQueues(queues: _*)

  def getQueue(queueName: String): Option[Queue] =
    Option(dbos.getQueue(queueName).orElse(null))

  def listQueues(): Seq[Queue] =
    dbos.listQueues().asScala.toSeq

  def registerQueue(name: String, options: dev.dbos.transact.workflow.QueueOptions): Unit =
    dbos.registerQueue(name, options)

  def updateQueue(name: String, options: dev.dbos.transact.workflow.QueueOptions): Unit =
    dbos.updateQueue(name, options)

  def deleteQueue(name: String): Boolean =
    dbos.deleteQueue(name)

  // ── Schedules ───────────────────────────────────────────────

  def createSchedule(schedule: WorkflowSchedule): Unit =
    dbos.createSchedule(schedule)

  def getSchedule(name: String): Option[WorkflowSchedule] =
    Option(dbos.getSchedule(name).orElse(null))

  def deleteSchedule(name: String): Unit =
    dbos.deleteSchedule(name)

  def pauseSchedule(name: String): Unit =
    dbos.pauseSchedule(name)

  def resumeSchedule(name: String): Unit =
    dbos.resumeSchedule(name)

  def listSchedules(
      status: Seq[ScheduleStatus] = null,
      workflowName: Seq[String] = null,
      namePrefix: Seq[String] = null
  ): Seq[WorkflowSchedule] =
    dbos
      .listSchedules(
        if (status == null) null else status.asJava,
        if (workflowName == null) null else workflowName.asJava,
        if (namePrefix == null) null else namePrefix.asJava
      )
      .asScala
      .toSeq

  def triggerSchedule[T, E <: Exception](scheduleName: String): WorkflowHandle[T, E] =
    dbos.triggerSchedule(scheduleName)

  def backfillSchedule(
      scheduleName: String,
      start: Instant,
      end: Instant
  ): Seq[WorkflowHandle[Object, Exception]] =
    dbos.backfillSchedule(scheduleName, start, end).asScala.toSeq

  def applySchedules(schedules: WorkflowSchedule*): Unit =
    dbos.applySchedules(schedules: _*)

  // ── Sleep ───────────────────────────────────────────────────

  def sleep(duration: Duration): Unit =
    dbos.sleep(duration)

  // ── Debouncer ───────────────────────────────────────────────

  def debouncer[R](): Debouncer[R] =
    dbos.debouncer()

  // ── Patching ────────────────────────────────────────────────

  def patch(patchName: String): Boolean =
    dbos.patch(patchName)

  def deprecatePatch(patchName: String): Boolean =
    dbos.deprecatePatch(patchName)

  // ── Streams ─────────────────────────────────────────────────

  def writeStream(key: String, value: Any): Unit =
    dbos.writeStream(key, value)

  def closeStream(key: String): Unit =
    dbos.closeStream(key)

  // ── Version Management ──────────────────────────────────────

  def listApplicationVersions(): Seq[VersionInfo] =
    dbos.listApplicationVersions().asScala.toSeq

  def getLatestApplicationVersion(): VersionInfo =
    dbos.getLatestApplicationVersion()

  def setLatestApplicationVersion(versionName: String): Unit =
    dbos.setLatestApplicationVersion(versionName)

  // ── Workflow Delay ──────────────────────────────────────────

  def setWorkflowDelay(workflowId: String, delay: Duration): Unit =
    dbos.setWorkflowDelay(workflowId, delay)

  def setWorkflowDelay(workflowId: String, delayUntil: Instant): Unit =
    dbos.setWorkflowDelay(workflowId, delayUntil)
}

object ScalaDBOS {

  def apply(config: DBOSConfig): ScalaDBOS =
    new ScalaDBOS(new JDBOS(config))

  def workflowId: String = JDBOS.workflowId()

  def stepId: Integer = JDBOS.stepId()

  def inWorkflow: Boolean = JDBOS.inWorkflow()

  def inStep: Boolean = JDBOS.inStep()

  def serializationStrategy: SerializationStrategy =
    JDBOS.serializationStrategy()

  def version: String = JDBOS.version()
}
