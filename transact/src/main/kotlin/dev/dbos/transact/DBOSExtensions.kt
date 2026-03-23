@file:JvmName("DbosKotlinInternal") // Hides the file from casual Java view
@file:JvmSynthetic

package dev.dbos.transact

import dev.dbos.transact.execution.ThrowingSupplier
import dev.dbos.transact.workflow.StepOptions
import dev.dbos.transact.workflow.WorkflowHandle

// Note: there is no Kotlin extension for startWorkflow with just a lambda parameter.
// Kotlin prefers members over extensions, and automatically SAM-converts lambdas to functional
// interfaces (like ThrowingSupplier), so such an extension would be unreachable — the Java member
// wins before the extension is considered. This does not affect overloads that also take a
// StartWorkflowOptions parameter, since those signatures are distinct from any Java member.
// For the no-options case, we provide beginWorkflow as a uniquely-named alternative.

/**
 * Starts a workflow using trailing lambda syntax, with the given [options].
 *
 * This is a Kotlin-friendly alternative to [DBOS.startWorkflow] that places the lambda last,
 * enabling trailing lambda call syntax. The exception type is fixed to [Exception].
 *
 * @param T the return type of the workflow
 * @param options (nullable) workflow options such as workflow ID
 * @param block the workflow body to execute
 * @return a [WorkflowHandle] for retrieving the workflow result
 */
@JvmSynthetic
fun <T> DBOS.startWorkflow(
  options: StartWorkflowOptions?,
  block: () -> T,
): WorkflowHandle<T, Exception> {
  return this.startWorkflow(
    ThrowingSupplier<T, Exception> { block() },
    options ?: StartWorkflowOptions(),
  )
}

/**
 * Runs a step using trailing lambda syntax, with the given [options].
 *
 * This is a Kotlin-friendly alternative to [DBOS.runStep] that places the lambda last, enabling
 * trailing lambda call syntax. The exception type is fixed to [Exception].
 *
 * @param T the return type of the step
 * @param options step options such as step name and retry policy
 * @param block the step body to execute
 * @return the result of the step
 */
@JvmSynthetic
fun <T> DBOS.runStep(options: StepOptions, block: () -> T): T {
  return this.runStep(ThrowingSupplier<T, Exception> { block() }, options)
}

/**
 * Runs a step using trailing lambda syntax, with the given [name].
 *
 * This is a Kotlin-friendly alternative to [DBOS.runStep] that places the lambda last, enabling
 * trailing lambda call syntax. The exception type is fixed to [Exception].
 *
 * @param T the return type of the step
 * @param name the name of the step
 * @param block the step body to execute
 * @return the result of the step
 */
@JvmSynthetic
fun <T> DBOS.runStep(name: String, block: () -> T): T {
  return this.runStep(ThrowingSupplier<T, Exception> { block() }, name)
}
