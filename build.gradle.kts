import com.vanniktech.maven.publish.DeploymentValidation

plugins {
  id("pmd")
  alias(libs.plugins.spotless)
  alias(libs.plugins.versions)
  alias(libs.plugins.maven.publish) apply false
  alias(libs.plugins.kotlin.jvm) apply false
}

fun runCommand(vararg args: String): String {
  val process = ProcessBuilder(*args).directory(rootDir).redirectErrorStream(true).start()

  val output = process.inputStream.bufferedReader().readText()
  val exitCode = process.waitFor()

  if (exitCode != 0) {
    throw GradleException("Command failed with exit code $exitCode: ${args.joinToString(" ")}")
  }

  return output.trim()
}

val gitHash: String by lazy { runCommand("git", "rev-parse", "--short", "HEAD") }

val gitTag: String? by lazy {
  runCatching { runCommand("git", "describe", "--abbrev=0", "--tags") }.getOrNull()
}

val commitCount: Int by lazy {
  val range = if (gitTag.isNullOrEmpty()) "HEAD" else "$gitTag..HEAD"
  runCommand("git", "rev-list", "--count", range).toInt()
}

val branch: String by lazy {
  // First, try GitHub Actions environment variable
  val githubBranch = System.getenv("GITHUB_REF_NAME")
  if (!githubBranch.isNullOrBlank()) githubBranch

  // Fallback to local git command
  else {
    runCommand("git", "rev-parse", "--abbrev-ref", "HEAD")
  }
}

fun parseTag(tag: String): Triple<Int, Int, Int>? {
  val regex = Regex("""v?(\d+)\.(\d+)\.(\d+)""")
  val match = regex.matchEntire(tag.trim()) ?: return null
  val (major, minor, patch) = match.destructured
  return Triple(major.toInt(), minor.toInt(), patch.toInt())
}

fun calcVersion(): String {
  var (major, minor, patch) = parseTag(gitTag ?: "") ?: Triple(0, 1, 0)

  if (branch == "main") {
    return "$major.${minor + 1}.$patch-m$commitCount"
  }

  if (branch.startsWith("release/v")) {
    if (commitCount == 0) {
      return "$major.$minor.$patch"
    } else {
      return "$major.$minor.${patch + 1}-rc$commitCount"
    }
  }

  return "$major.${minor + 1}.$patch-a$commitCount-g$gitHash"
}

val calculatedVersion: String by lazy { calcVersion() }

// prints when Gradle evaluates the build
println("DBOS Transact version: $calculatedVersion")

allprojects {
  group = "dev.dbos"
  version = calculatedVersion
  extra["commitCount"] = "$commitCount"

  repositories {
    mavenCentral()
    gradlePluginPortal()
  }
}

spotless {
  kotlinGradle {
    target("*.gradle.kts")
    ktfmt("0.61").googleStyle()
    trimTrailingWhitespace()
    endWithNewline()
  }
}

val pmdVersion = libs.versions.pmd.get()

extensions.configure<org.gradle.api.plugins.quality.PmdExtension> { toolVersion = pmdVersion }

subprojects {
  apply(plugin = "java")
  apply(plugin = "pmd")
  apply(plugin = "com.diffplug.spotless")
  apply(plugin = "com.github.ben-manes.versions")

  // PMD configuration
  extensions.configure<org.gradle.api.plugins.quality.PmdExtension> {
    toolVersion = pmdVersion
    ruleSets = listOf() // disable defaults
    ruleSetFiles = files("${rootDir}/config/pmd/ruleset.xml")
    isConsoleOutput = true
  }

  // Spotless configuration
  extensions.configure<com.diffplug.gradle.spotless.SpotlessExtension> {
    java {
      googleJavaFormat()
      cleanthat()
      formatAnnotations()
      removeUnusedImports()
      importOrder("dev.dbos", "java", "javax", "")
      trimTrailingWhitespace()
      endWithNewline()
    }
    kotlin {
      target("**/*.kt")
      targetExclude("build/**/*.kt")
      ktfmt("0.62").googleStyle()
      trimTrailingWhitespace()
      endWithNewline()
    }
    kotlinGradle {
      target("**/*.gradle.kts")
      ktfmt("0.62").googleStyle()
      trimTrailingWhitespace()
      endWithNewline()
    }
  }

  tasks.withType<com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask> {
    rejectVersionIf {
      fun isUnstable(version: String) =
        listOf("alpha", "beta", "rc", "cr", "m", "preview", "b", "ea").any { qualifier ->
          version.lowercase().contains(qualifier)
        }
      isUnstable(candidate.version) && !isUnstable(currentVersion)
    }
  }

  plugins.withId("java") {
    // Force the published bytecode to be Java 17
    extensions.getByType<JavaPluginExtension>().apply {
      sourceCompatibility = JavaVersion.VERSION_17
      targetCompatibility = JavaVersion.VERSION_17
    }

    tasks.withType<JavaCompile> {
      options.release.set(17)
      options.compilerArgs.addAll(
        listOf("-Xlint:unchecked", "-Xlint:deprecation", "-Xlint:rawtypes", "-Werror")
      )
    }

    // use the environment's JDK instead of the toolchain's JDK for tests
    tasks.withType<Test> { javaLauncher.set(null as JavaLauncher?) }

    tasks.withType<Test> {
      useJUnitPlatform { if (System.getenv("CI") != "true") excludeTags("ci-only") }
      testLogging {
        events("failed")
        showStandardStreams = true
        showExceptions = true
        showCauses = true
        showStackTraces = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
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
        }
      )
    }

    tasks.named<Jar>("jar") {
      manifest {
        attributes["Implementation-Version"] = project.version
        attributes["Implementation-Title"] = project.name
        attributes["Implementation-Vendor"] = "DBOS, Inc"
        attributes["Implementation-Vendor-Id"] = project.group
        attributes["SCM-Revision"] = gitHash
      }
    }
  }

  plugins.withId("com.vanniktech.maven.publish") {
    val publishingToMavenCentral =
      gradle.startParameter.taskNames.any { it.contains("publishToMavenCentral") }

    tasks.withType<Javadoc> {
      (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:all,-missing", "-quiet")
        encoding = "UTF-8"
      }
    }

    tasks.named("build") { dependsOn("javadoc") }

    extensions.configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
      publishToMavenCentral(automaticRelease = true, validateDeployment = DeploymentValidation.NONE)
      if (publishingToMavenCentral) signAllPublications()

      pom {
        inceptionYear.set("2025")
        url.set("https://github.com/dbos-inc/dbos-transact-java")

        licenses {
          license {
            name.set("MIT License")
            url.set("https://opensource.org/licenses/MIT")
          }
        }

        developers {
          developer {
            id.set("dbos-inc")
            name.set("DBOS Inc")
            email.set("support@dbos.dev")
          }
        }

        scm {
          connection.set("scm:git:git://github.com/dbos-inc/dbos-transact-java.git")
          developerConnection.set("scm:git:ssh://github.com:dbos-inc/dbos-transact-java.git")
          url.set("https://github.com/dbos-inc/dbos-transact-java/tree/main")
        }
      }
    }
  }
}
