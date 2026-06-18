plugins {
  id("java-library")
  id("dbos.java-conventions")
  id("dbos.kotlin-conventions")
  id("dbos.quality-conventions")
  id("dbos.publishing-conventions")
}

// printed once when Gradle configures this project
println("DBOS Transact version: $version")

dependencies {
  api(libs.jspecify)

  implementation(libs.cron.utils)
  implementation(libs.hikaricp)
  implementation(libs.jackson.databind)
  implementation(libs.postgresql)
  implementation(libs.slf4j.api)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.junit.platform.engine)
  testImplementation(libs.system.stubs.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)

  testImplementation(libs.java.websocket)
  testImplementation(libs.kryo)
  testImplementation(libs.logback.classic)
  testImplementation(libs.maven.artifact)
  testImplementation(libs.mockito.core)
  testImplementation(libs.rest.assured)
  testImplementation(libs.sqlite.jdbc)
  testImplementation(libs.testcontainers.cockroachdb)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(libs.testcontainers.toxiproxy)
}

val projectVersion = project.version.toString()

tasks.processResources {
  inputs.property("version", projectVersion)

  filesMatching("**/app.properties") { expand(mapOf("projectVersion" to projectVersion)) }
}

mavenPublishing {
  pom {
    name.set("DBOS Transact")
    description.set("DBOS Transact Java SDK for lightweight durable workflows")
  }
}
