plugins {
  application
  id("com.gradleup.shadow") version "9.3.2"
}

application { mainClass.set("dev.dbos.transact.cli.Main") }

dependencies {
  implementation(project(":transact"))
  implementation("com.fasterxml.jackson.core:jackson-databind:2.21.1")
  implementation("info.picocli:picocli:4.7.7")
  runtimeOnly("org.slf4j:slf4j-simple:2.0.17")

  testImplementation(platform("org.junit:junit-bom:6.0.3"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
  testImplementation("org.testcontainers:testcontainers-postgresql:2.0.3")
}

tasks.test {
  useJUnitPlatform()
  testLogging {
    events("passed", "skipped", "failed")
    showStandardStreams = true
  }

  addTestListener(
    object : TestListener {
      override fun beforeSuite(suite: TestDescriptor) {}

      override fun beforeTest(testDescriptor: TestDescriptor) {}

      override fun afterTest(testDescriptor: TestDescriptor, result: TestResult) {}

      override fun afterSuite(suite: TestDescriptor, result: TestResult) {
        if (suite.parent == null) {
          println("\nTest Results:")
          println("  Tests run: ${result.testCount}")
          println("  Passed: ${result.successfulTestCount}")
          println("  Failed: ${result.failedTestCount}")
          println("  Skipped: ${result.skippedTestCount}")
        }
      }
    }
  )
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
  archiveBaseName.set("dbos")
  archiveVersion.set("")
  archiveClassifier.set("")
}

tasks.withType<JavaCompile> {
  options.compilerArgs.add("-Xlint:unchecked") // warn about unchecked operations
  options.compilerArgs.add("-Xlint:deprecation") // warn about deprecated APIs
  options.compilerArgs.add("-Xlint:rawtypes") // warn about raw types
  options.compilerArgs.add("-Werror") // treat all warnings as errors
}
