@file:JvmName("DbosKotlinInternal") // Hides the file from casual Java view
@file:JvmSynthetic

package dev.dbos.transact

import dev.dbos.transact.execution.ThrowingRunnable
import dev.dbos.transact.execution.ThrowingSupplier
import dev.dbos.transact.execution.DBOSLifecycleListener
import dev.dbos.transact.workflow.WorkflowHandle
import dev.dbos.transact.workflow.StepOptions
import dev.dbos.transact.workflow.ForkOptions
import dev.dbos.transact.workflow.Queue
import dev.dbos.transact.workflow.ListWorkflowsInput
import dev.dbos.transact.registerLifecycleListener
import dev.dbos.transact.config.DBOSConfig
import dev.dbos.transact.listWorkflowSteps
import dev.dbos.transact.database.ExternalState
import java.time.Duration

/**
 * Marks this as 'synthetic' so Java compilers ignore it, 
 * but Kotlin compilers see it perfectly.
 */
@JvmSynthetic
inline fun <reified T : Any> registerWorkflows(implementation: T, instanceName: String = ""): T {
    return DBOS.registerWorkflows(T::class.java, implementation, instanceName)
}

@JvmSynthetic
fun registerQueue(queue: Queue) = DBOS.registerQueue(queue)

@JvmSynthetic
fun registerLifecycleListener(listener: DBOSLifecycleListener) = DBOS.registerLifecycleListener(listener)

@JvmSynthetic
fun configure(config: DBOSConfig) = DBOS.configure(config)

@JvmSynthetic
fun launch() = DBOS.launch()

@JvmSynthetic
fun shutdown() = DBOS.shutdown()

@get:JvmSynthetic
val workflowId: String?
    get() = DBOS.workflowId()

@get:JvmSynthetic
val stepId: Int?
    get() = DBOS.stepId()

@get:JvmSynthetic
val inWorkflow: Boolean
    get() = DBOS.inWorkflow()

@get:JvmSynthetic
val inStep: Boolean
    get() = DBOS.inStep()

@JvmSynthetic
fun getQueue(queueName: String) = DBOS.getQueue(queueName)

@JvmSynthetic
fun sleep(duration: Duration) = DBOS.sleep(duration)

@JvmSynthetic
fun <T> startWorkflow(block: () -> T): WorkflowHandle<T, Exception> {
    return DBOS.startWorkflow(ThrowingSupplier<T, Exception> { block() })
}

@JvmSynthetic
fun <T> startWorkflow(workflowId: String, block: () -> T): WorkflowHandle<T, Exception> {
    return DBOS.startWorkflow(ThrowingSupplier<T, Exception> { block() }, StartWorkflowOptions(workflowId))
}

@JvmSynthetic
fun <T> startWorkflow(options: StartWorkflowOptions, block: () -> T): WorkflowHandle<T, Exception> {
    return DBOS.startWorkflow(ThrowingSupplier<T, Exception> { block() }, options)
}

@JvmSynthetic
@JvmName("startWorkflowUnit")
fun startWorkflow(block: () -> Unit): WorkflowHandle<Void, Exception> {
    return DBOS.startWorkflow(ThrowingRunnable<Exception> { block() })
}

@JvmSynthetic
@JvmName("startWorkflowUnit")
fun startWorkflow(workflowId: String, block: () -> Unit): WorkflowHandle<Void, Exception> {
    return DBOS.startWorkflow(ThrowingRunnable<Exception> { block() }, StartWorkflowOptions(workflowId))
}

@JvmSynthetic
@JvmName("startWorkflowUnit")
fun startWorkflow(options: StartWorkflowOptions, block: () -> Unit): WorkflowHandle<Void, Exception> {
    return DBOS.startWorkflow(ThrowingRunnable<Exception> { block() }, options)
}

@JvmSynthetic
@Throws(Exception::class)
fun <T> getResult(workflowId: String) = DBOS.getResult<T, Exception>(workflowId)

@JvmSynthetic
fun getWorkflowStatus(workflowId: String) = DBOS.getWorkflowStatus(workflowId)

@JvmSynthetic
fun send(destinationId: String, message: Any, topic: String, idempotencyKey: String? = null) {
    DBOS.send(destinationId, message, topic, idempotencyKey)
}

@JvmSynthetic
fun recv(topic: String, timeout: Duration) = DBOS.recv(topic, timeout)

@JvmSynthetic
fun setEvent(key: String, value: Any) = DBOS.setEvent(key, value)

@JvmSynthetic
fun getEvent(workflowId: String, key: String, timeout: Duration) = DBOS.getEvent(workflowId, key, timeout)

@JvmSynthetic
fun runStep(name: String, block: () -> Unit) = DBOS.runStep(ThrowingRunnable { block() }, StepOptions(name))

@JvmSynthetic
fun runStep(options: StepOptions, block: () -> Unit) = DBOS.runStep(ThrowingRunnable { block() }, options)

@JvmSynthetic
fun <T> runStep(name: String, block: () -> T) = DBOS.runStep(ThrowingSupplier<T, Exception> { block() }, StepOptions(name))

@JvmSynthetic
fun <T> runStep(options: StepOptions, block: () -> T) = DBOS.runStep(ThrowingSupplier<T, Exception> { block() }, options)

@JvmSynthetic
fun <T> resumeWorkflow(workflowId: String) = DBOS.resumeWorkflow<T, Exception>(workflowId)

@JvmSynthetic
fun cancelWorkflow(workflowId: String) = DBOS.cancelWorkflow(workflowId)

@JvmSynthetic
fun <T> forkWorkflow(
    workflowId: String, 
    startStep: Int, 
    options: ForkOptions = ForkOptions()
) = DBOS.forkWorkflow<T, Exception>(workflowId, startStep, options)

@JvmSynthetic
fun <T> retrieveWorkflow(workflowId: String) = DBOS.retrieveWorkflow<T, Exception>(workflowId)

@JvmSynthetic
fun listWorkflows(input: ListWorkflowsInput) = DBOS.listWorkflows(input)

@JvmSynthetic
fun listWorkflowSteps(workflowId: String) = DBOS.listWorkflowSteps(workflowId)

@JvmSynthetic
fun getRegisteredWorkflows() = DBOS.getRegisteredWorkflows()

@JvmSynthetic
fun getRegisteredWorkflowInstances() = DBOS.getRegisteredWorkflowInstances()

@JvmSynthetic
fun getExternalState(service: String, workflowName: String, key: String) = DBOS.getExternalState(service, workflowName, key)

@JvmSynthetic
fun upsertExternalState(state: ExternalState) = DBOS.upsertExternalState(state)

@JvmSynthetic
fun patch(name: String) = DBOS.patch(name)

@JvmSynthetic
fun deprecatePatch(name: String) = DBOS.deprecatePatch(name)