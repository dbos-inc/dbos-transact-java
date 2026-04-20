import com.vanniktech.maven.publish.DeploymentValidation
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
  id("java")
  id("java-library")
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.maven.publish)
}

tasks.withType<JavaCompile> {
  options.compilerArgs.add("-Xlint:unchecked") // warn about unchecked operations
  options.compilerArgs.add("-Xlint:deprecation") // warn about deprecated APIs
  options.compilerArgs.add("-Xlint:rawtypes") // warn about raw types
  options.compilerArgs.add("-Werror") // treat all warnings as errors
}

tasks.withType<Javadoc> {
  (options as StandardJavadocDocletOptions).apply {
    addStringOption("Xdoclint:all,-missing", "-quiet") // hide warnings for missing javadoc comments
    encoding = "UTF-8" // optional, ensures UTF-8 for docs
  }
}

tasks.named("build") { dependsOn("javadoc") }

dependencies {
  api(libs.slf4j.api)
  api(libs.jspecify)

  implementation(libs.postgresql)
  implementation(libs.hikaricp)
  implementation(libs.bundles.jackson)
  implementation(libs.cron.utils)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.system.stubs.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)

  testImplementation(libs.java.websocket)
  testImplementation(libs.logback.classic)
  testImplementation(libs.mockito.core)
  testImplementation(libs.rest.assured)
  testImplementation(libs.maven.artifact)
  testImplementation(libs.testcontainers.postgresql)
}

val projectVersion = project.version.toString()

tasks.processResources {
  inputs.property("version", projectVersion)

  filesMatching("**/app.properties") { expand(mapOf("projectVersion" to projectVersion)) }
}

tasks.withType<KotlinCompile>().configureEach {
  compilerOptions {
    // jvmTarget now uses the JvmTarget enum instead of a String
    jvmTarget.set(JvmTarget.JVM_17)

    // freeCompilerArgs is now a Property/ListProperty, so we use .add() or .addAll()
    freeCompilerArgs.add("-Xjsr305=strict")
  }
}

val publishingToMavenCentral =
  gradle.startParameter.taskNames.any { it.contains("publishToMavenCentral") }

mavenPublishing {
  publishToMavenCentral(automaticRelease = true, validateDeployment = DeploymentValidation.NONE)
  if (publishingToMavenCentral) {
    signAllPublications()
  }

  pom {
    name.set("DBOS Transact")
    description.set("DBOS Transact Java SDK for lightweight durable workflows")
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
