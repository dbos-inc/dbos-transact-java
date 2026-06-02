package dev.dbos.transact.scala

import dev.dbos.transact.context.WorkflowOptions
import dev.dbos.transact.utils.PgContainer
import org.junit.jupiter.api.{AutoClose, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import scala.jdk.CollectionConverters.*

class ScalaDBOSSpec {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  @AutoClose final val pgContainer: PgContainer = new PgContainer()

  private var dbos: ScalaDBOS = _

  @BeforeEach
  def beforeEach(): Unit = {
    val config = pgContainer.dbosConfig("scala-binding-test")
    dbos = ScalaDBOS(config)
  }

  // ────────────────────────────────────────────────────────────
  // 1. Basic lifecycle + inline steps
  // ────────────────────────────────────────────────────────────

  @Test
  def `lifecycle and inline steps`(): Unit = {
    dbos.launch()

    val wfId = "test-inline-steps"

    val handle = dbos.startWorkflow({
      val s1 = dbos.runStep("step-one") {
        log.info("step one running")
        "hello"
      }
      val s2 = dbos.runStep("step-two") {
        log.info("step two running")
        "world"
      }
      s1 + " " + s2
    }, new dev.dbos.transact.StartWorkflowOptions(wfId))

    val result = handle.getResult
    assertEquals("hello world", result)

    val steps = dbos.listWorkflowSteps(wfId)
    assertEquals(2, steps.size)
    assertEquals("step-one", steps(0).functionName)
    assertEquals("step-two", steps(1).functionName)
  }

  // ────────────────────────────────────────────────────────────
  // 2. Reflection-based workflow registration (registerWorkflow)
  //    Mirrors the Clojure binding's approach
  // ────────────────────────────────────────────────────────────

  @Test
  def `reflection-based workflow registration`(): Unit = {
    val stepsEvent = "steps_event"

    def stepOne(): Unit = {
      log.info("Reflective workflow step 1")
    }

    def stepTwo(): Unit = {
      log.info("Reflective workflow step 2")
    }

    val exampleWorkflow: ScalaDBOS => String = (d: ScalaDBOS) => {
      d.runStep("stepOne")(stepOne())
      d.setEvent(stepsEvent, 1)
      d.runStep("stepTwo")(stepTwo())
      d.setEvent(stepsEvent, 2)
      "done"
    }

    dbos.launch()

    val startWorkflow = dbos.registerWorkflow("exampleWorkflow")(exampleWorkflow)
    val result = startWorkflow("reflective-wf")
    assertEquals("done", result)

    val event1 = dbos.getEvent[Int]("reflective-wf", stepsEvent, Duration.ofSeconds(5))
    assertTrue(event1.isDefined)
    assertEquals(2, event1.get)
  }

  // ────────────────────────────────────────────────────────────
  // 3. Zero-arg closure registration (registerWorkflow0)
  // ────────────────────────────────────────────────────────────

  @Test
  def `zero-arg closure workflow registration`(): Unit = {
    val captured = "captured-value"

    val workflowFn: () => String = () => {
      dbos.runStep("capture-step") {
        captured
      }
    }

    dbos.launch()

    val startWorkflow = dbos.registerWorkflow0("closureWorkflow")(workflowFn)
    val result = startWorkflow("closure-wf")
    assertEquals("captured-value", result)
  }

  // ────────────────────────────────────────────────────────────
  // 4. Step error handling
  // ────────────────────────────────────────────────────────────

  @Test
  def `step error handling`(): Unit = {
    val workflowFn: ScalaDBOS => String = (d: ScalaDBOS) => {
      d.runStep("good-step") {
        "ok"
      }
      try {
        d.runStep("failing-step") {
          throw new RuntimeException("step failed!")
        }
      } catch {
        case e: RuntimeException =>
          log.info("Caught expected error: {}", e.getMessage)
      }
      d.runStep("after-failure") {
        "recovered"
      }
    }

    dbos.launch()

    val startWorkflow = dbos.registerWorkflow("errorHandlingWF")(workflowFn)
    val result = startWorkflow("error-wf")
    assertEquals("recovered", result)

    val steps = dbos.listWorkflowSteps("error-wf")
    assertEquals(3, steps.size)
    assertNotNull(steps(1).error)
    assertEquals("recovered", steps(2).output)
  }

  // ────────────────────────────────────────────────────────────
  // 5. Workflow queries (status, steps, events)
  // ────────────────────────────────────────────────────────────

  @Test
  def `workflow queries`(): Unit = {
    val workflowFn: ScalaDBOS => String = (d: ScalaDBOS) => {
      d.runStep("query-step") {
        d.setEvent("evt", 42)
        "step-result"
      }
    }

    dbos.launch()

    val startWorkflow = dbos.registerWorkflow("queryWorkflow")(workflowFn)
    val handle = dbos.retrieveWorkflow[String, Exception]("query-wf")

    startWorkflow("query-wf")
    val result = handle.getResult
    assertEquals("step-result", result)

    val status = dbos.getWorkflowStatus("query-wf")
    assertTrue(status.isDefined)

    val evt = dbos.getEvent[Int]("query-wf", "evt", Duration.ofSeconds(5))
    assertTrue(evt.isDefined)
    assertEquals(42, evt.get)

    val steps = dbos.listWorkflowSteps("query-wf")
    assertEquals(1, steps.size)
    assertEquals("query-step", steps(0).functionName)
  }

  // ────────────────────────────────────────────────────────────
  // 6. Multiple concurrent workflows
  // ────────────────────────────────────────────────────────────

  @Test
  def `concurrent workflow executions`(): Unit = {
    val workflowFn: ScalaDBOS => String = (d: ScalaDBOS) => {
      d.runStep("concat-step") {
        "result"
      }
    }

    dbos.launch()
    val startWorkflow = dbos.registerWorkflow("concurrentWF")(workflowFn)

    val r1 = startWorkflow("conc-wf-1")
    val r2 = startWorkflow("conc-wf-2")

    assertEquals("result", r1)
    assertEquals("result", r2)
  }

  // ────────────────────────────────────────────────────────────
  // 7. Static methods
  // ────────────────────────────────────────────────────────────

  @Test
  def `static methods accessible`(): Unit = {
    val version = ScalaDBOS.version
    assertNotNull(version)
    assertTrue(version.nonEmpty)

    // these are null when called outside a workflow
    assertTrue(ScalaDBOS.workflowId == null)
    assertTrue(ScalaDBOS.stepId == null)
    assertFalse(ScalaDBOS.inWorkflow)
    assertFalse(ScalaDBOS.inStep)
  }
}
