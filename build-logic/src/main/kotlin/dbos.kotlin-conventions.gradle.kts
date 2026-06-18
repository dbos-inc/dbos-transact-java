import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

// Kotlin is applied here (rather than via the module's own plugins block) so that it shares
// build-logic's classloader with the publish plugin, which inspects the Kotlin plugin when
// configuring publications.
plugins { id("org.jetbrains.kotlin.jvm") }

tasks.withType<KotlinCompile>().configureEach {
  compilerOptions {
    jvmTarget.set(JvmTarget.JVM_17)
    freeCompilerArgs.add("-Xjsr305=strict")
  }
}
