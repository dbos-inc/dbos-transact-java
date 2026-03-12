@file:JvmName("DbosKotlinInternal") // Hides the file from casual Java view
@file:JvmSynthetic

package dev.dbos.transact

import dev.dbos.transact.execution.ThrowingRunnable
import dev.dbos.transact.execution.ThrowingSupplier
import dev.dbos.transact.workflow.WorkflowHandle
import dev.dbos.transact.workflow.StepOptions
import dev.dbos.transact.StartWorkflowOptions

/**
 * Kotlin extension methods for DBOS.startWorkflow that put lambda parameters last
 * for better Kotlin ergonomics with trailing lambda syntax.
 */

/**
 * Start workflow with options, lambda parameter last for trailing lambda syntax
 */
@JvmSynthetic
fun <T, E : Exception> DBOS.startWorkflow(
    options: StartWorkflowOptions,
    supplier: ThrowingSupplier<T, E>
): WorkflowHandle<T, E> {
    return startWorkflow(supplier, options)
}

/**
 * Start workflow with runnable and options, lambda parameter last for trailing lambda syntax
 */
@JvmSynthetic
fun <E : Exception> DBOS.startWorkflow(
    options: StartWorkflowOptions,
    runnable: ThrowingRunnable<E>
): WorkflowHandle<Void, E> {
    return startWorkflow(runnable, options)
}

/**
 * Convenient Kotlin extension for starting workflow with options and Kotlin lambda
 */
@JvmSynthetic
inline fun <T> DBOS.startWorkflow(
    options: StartWorkflowOptions,
    crossinline block: () -> T
): WorkflowHandle<T, Exception> {
    return startWorkflow(options, ThrowingSupplier<T, Exception> { block() })
}

/**
 * Convenient Kotlin extension for starting workflow with Kotlin lambda (default options)
 */
@JvmSynthetic
inline fun <T> DBOS.startWorkflow(
    crossinline block: () -> T
): WorkflowHandle<T, Exception> {
    return startWorkflow(ThrowingSupplier<T, Exception> { block() })
}

/**
 * Convenient Kotlin extension for starting workflow with options and Unit-returning Kotlin lambda
 */
@JvmSynthetic
@JvmName("startWorkflowUnit")
inline fun DBOS.startWorkflow(
    options: StartWorkflowOptions,
    crossinline block: () -> Unit
): WorkflowHandle<Void, Exception> {
    return startWorkflow(options, ThrowingRunnable<Exception> { block() })
}

/**
 * Convenient Kotlin extension for starting workflow with Unit-returning Kotlin lambda (default options)  
 */
@JvmSynthetic
@JvmName("startWorkflowUnit")
inline fun DBOS.startWorkflow(
    crossinline block: () -> Unit
): WorkflowHandle<Void, Exception> {
    return startWorkflow(ThrowingRunnable<Exception> { block() })
}

/**
 * Kotlin extension methods for DBOS.runStep that put lambda parameters last
 * for better Kotlin ergonomics with trailing lambda syntax.
 */

/**
 * Run step with options, lambda parameter last for trailing lambda syntax
 */
@JvmSynthetic
fun <T, E : Exception> DBOS.runStep(
    opts: StepOptions,
    stepfunc: ThrowingSupplier<T, E>
): T {
    return runStep(stepfunc, opts)
}

/**
 * Run step with name, lambda parameter last for trailing lambda syntax
 */
@JvmSynthetic
fun <T, E : Exception> DBOS.runStep(
    name: String,
    stepfunc: ThrowingSupplier<T, E>
): T {
    return runStep(stepfunc, name)
}

/**
 * Run step with options and runnable, lambda parameter last for trailing lambda syntax
 */
@JvmSynthetic
fun <E : Exception> DBOS.runStep(
    opts: StepOptions,
    stepfunc: ThrowingRunnable<E>
) {
    return runStep(stepfunc, opts)
}

/**
 * Run step with name and runnable, lambda parameter last for trailing lambda syntax
 */
@JvmSynthetic
fun <E : Exception> DBOS.runStep(
    name: String,
    stepfunc: ThrowingRunnable<E>
) {
    return runStep(stepfunc, name)
}

/**
 * Convenient Kotlin extension for running step with options and Kotlin lambda
 */
@JvmSynthetic
inline fun <T> DBOS.runStep(
    opts: StepOptions,
    crossinline block: () -> T
): T {
    return runStep(opts, ThrowingSupplier<T, Exception> { block() })
}

/**
 * Convenient Kotlin extension for running step with name and Kotlin lambda
 */
@JvmSynthetic
inline fun <T> DBOS.runStep(
    name: String,
    crossinline block: () -> T
): T {
    return runStep(name, ThrowingSupplier<T, Exception> { block() })
}

/**
 * Convenient Kotlin extension for running step with options and Unit-returning Kotlin lambda
 */
@JvmSynthetic
@JvmName("runStepUnit")
inline fun DBOS.runStep(
    opts: StepOptions,
    crossinline block: () -> Unit
) {
    return runStep(opts, ThrowingRunnable<Exception> { block() })
}

/**
 * Convenient Kotlin extension for running step with name and Unit-returning Kotlin lambda
 */
@JvmSynthetic
@JvmName("runStepUnit")
inline fun DBOS.runStep(
    name: String,
    crossinline block: () -> Unit
) {
    return runStep(name, ThrowingRunnable<Exception> { block() })
}
