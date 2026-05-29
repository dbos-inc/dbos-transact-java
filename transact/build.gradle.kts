import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
  id("java-library")
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.maven.publish)
}

dependencies {
  api(libs.slf4j.api)
  api(libs.jspecify)

  implementation(libs.asm)
  implementation(libs.postgresql)
  implementation(libs.hikaricp)
  implementation(libs.bundles.jackson)
  implementation(libs.cron.utils)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.system.stubs.jupiter)
  testImplementation(libs.junit.platform.engine)
  testRuntimeOnly(libs.junit.platform.launcher)

  testImplementation(libs.java.websocket)
  testImplementation(libs.logback.classic)
  testImplementation(libs.mockito.core)
  testImplementation(libs.sqlite.jdbc)
  testImplementation(libs.rest.assured)
  testImplementation(libs.kryo)
  testImplementation(libs.maven.artifact)
  testImplementation(libs.testcontainers.cockroachdb)
  testImplementation(libs.testcontainers.postgresql)
}

val projectVersion = project.version.toString()

tasks.processResources {
  inputs.property("version", projectVersion)

  filesMatching("**/app.properties") { expand(mapOf("projectVersion" to projectVersion)) }
}

tasks.withType<KotlinCompile>().configureEach {
  compilerOptions {
    jvmTarget.set(JvmTarget.JVM_17)
    freeCompilerArgs.add("-Xjsr305=strict")
  }
}

mavenPublishing {
  pom {
    name.set("DBOS Transact")
    description.set("DBOS Transact Java SDK for lightweight durable workflows")
  }
}
