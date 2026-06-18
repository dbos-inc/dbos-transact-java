import dev.dbos.build.GitVersion
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins { java }

group = "dev.dbos"
version = GitVersion.version(rootDir)

// Force the published bytecode to be Java 17
extensions.getByType<JavaPluginExtension>().apply {
  sourceCompatibility = JavaVersion.VERSION_17
  targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<JavaCompile>().configureEach {
  options.release.set(17)
  options.compilerArgs.addAll(
    listOf("-Xlint:unchecked", "-Xlint:deprecation", "-Xlint:rawtypes", "-Werror"))
}

tasks.withType<Test>().configureEach {
  // use the environment's JDK instead of the toolchain's JDK for tests
  javaLauncher.set(null as JavaLauncher?)

  useJUnitPlatform { if (System.getenv("CI") != "true") excludeTags("ci-only") }
  testLogging {
    events("failed")
    showStandardStreams = true
    showExceptions = true
    showCauses = true
    showStackTraces = true
    exceptionFormat = TestExceptionFormat.FULL
  }
  addTestListener(
    object : TestListener {
      private val failedTests = mutableListOf<String>()

      override fun beforeSuite(suite: TestDescriptor) {}

      override fun beforeTest(testDescriptor: TestDescriptor) {}

      override fun afterTest(testDescriptor: TestDescriptor, result: TestResult) {
        if (result.resultType == TestResult.ResultType.FAILURE) {
          failedTests.add("${testDescriptor.className}.${testDescriptor.name}")
        }
      }

      override fun afterSuite(suite: TestDescriptor, result: TestResult) {
        if (suite.parent == null) {
          println("\nTest Results:")
          println("  Tests run: ${result.testCount}")
          println("  Passed: ${result.successfulTestCount}")
          println("  Failed: ${result.failedTestCount}")
          println("  Skipped: ${result.skippedTestCount}")

          if (failedTests.isNotEmpty()) {
            println("\nFailed Tests:")
            failedTests.forEach { println("  - $it") }
          }
        }
      }
    })
}

tasks.named<Jar>("jar") {
  manifest {
    attributes["Implementation-Version"] = project.version
    attributes["Implementation-Title"] = project.name
    attributes["Implementation-Vendor"] = "DBOS, Inc"
    attributes["Implementation-Vendor-Id"] = project.group
    attributes["SCM-Revision"] = GitVersion.gitHash(rootDir)
  }
}
