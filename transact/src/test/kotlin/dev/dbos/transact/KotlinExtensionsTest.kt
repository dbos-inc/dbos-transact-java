package dev.dbos.transact

import dev.dbos.transact.config.DBOSConfig
import dev.dbos.transact.utils.PgContainer
import dev.dbos.transact.workflow.Step
import dev.dbos.transact.workflow.StepOptions
import dev.dbos.transact.workflow.Workflow
import dev.dbos.transact.workflow.WorkflowHandle
import org.junit.jupiter.api.AutoClose
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.util.concurrent.TimeUnit

@Timeout(value = 2, unit = TimeUnit.MINUTES)
@Execution(ExecutionMode.CONCURRENT)
class KotlinExtensionsTest {

    @AutoClose
    private val pgContainer = PgContainer()
    
    private lateinit var dbosConfig: DBOSConfig
    
    @AutoClose 
    private lateinit var dbos: DBOS
    
    private lateinit var impl: KotlinTestServiceImpl
    private lateinit var proxy: KotlinTestService

    @BeforeEach
    fun beforeEach() {
        dbosConfig = pgContainer.dbosConfig()
        dbos = DBOS(dbosConfig)
        
        impl = KotlinTestServiceImpl(dbos)
        proxy = dbos.registerWorkflows(KotlinTestService::class.java, impl)
        impl.setProxy(proxy)
        
        dbos.launch()
    }

    @Test
    fun testStartWorkflowWithTrailingLambda() {
        // Test beginWorkflow with trailing lambda (default options)
        val handle1 = dbos.beginWorkflow(StartWorkflowOptions()) {
            proxy.simpleWorkflow("test")
        }
        
        val result1 = handle1.result
        assertEquals("testtest", result1)
        assertEquals(1, impl.stepCount)
    }

    @Test 
    fun testStartWorkflowWithOptionsAndTrailingLambda() {
        // Test beginWorkflow with options and trailing lambda
        val workflowId = "kotlin-test-${System.currentTimeMillis()}"
        val options = StartWorkflowOptions(workflowId)
        
        val handle = dbos.beginWorkflow(options) {
            proxy.workflowWithSteps("hello")
        }
        
        val result = handle.result
        assertEquals("hello_processed", result)
        assertEquals(workflowId, handle.workflowId())
        assertEquals(2, impl.stepCount) // Two steps in workflowWithSteps
    }

    @Test
    fun testStartWorkflowUnitWithTrailingLambda() {
        // Test beginWorkflow for Unit-returning workflow
        val handle: WorkflowHandle<Unit, Exception> = dbos.beginWorkflow(StartWorkflowOptions()) {
            proxy.unitWorkflow()
        }
        
        // Just verify it completes without error
        handle.result // This will throw if there was an exception
        assertEquals(1, impl.unitWorkflowCount)
    }

    @Test
    fun testRunStepWithTrailingLambda() {
        // This test demonstrates runStep usage within a workflow
        val handle = dbos.beginWorkflow(StartWorkflowOptions()) {
            proxy.workflowWithDirectSteps("kotlin")
        }
        
        val result = handle.result
        assertEquals("kotlin_step1_step2", result)
    }
}

interface KotlinTestService {
    fun simpleWorkflow(input: String): String
    fun workflowWithSteps(input: String): String  
    fun unitWorkflow()
    fun workflowWithDirectSteps(input: String): String
}

class KotlinTestServiceImpl(private val dbos: DBOS) : KotlinTestService {
    
    private lateinit var proxy: KotlinTestService
    
    var stepCount = 0
    var unitWorkflowCount = 0
    
    fun setProxy(proxy: KotlinTestService) {
        this.proxy = proxy
    }

    @Workflow
    override fun simpleWorkflow(input: String): String {
        // Use runStep with trailing lambda syntax
        return dbos.runStep("simpleStep") {
            stepCount++
            input + input
        }
    }

    @Workflow
    override fun workflowWithSteps(input: String): String {
        // Test both runStep with name and with options
        val step1Result = dbos.runStep("step1") {
            stepCount++
            input + "_processed"
        }
        
        return dbos.runStep(StepOptions("step2")) {
            stepCount++
            step1Result // Just pass through to verify it works
        }
    }

    @Workflow
    override fun unitWorkflow() {
        // Unit-returning workflow
        dbos.runStep("unitStep") {
            unitWorkflowCount++
        }
    }

    @Workflow  
    override fun workflowWithDirectSteps(input: String): String {
        // Demonstrate nested beginWorkflows using extension methods
        val result1 = dbos.runStep("directStep1") {
            stepCount++
            input + "_step1"
        }
        
        return dbos.runStep("directStep2") {
            stepCount++
            result1 + "_step2"  
        }
    }
}