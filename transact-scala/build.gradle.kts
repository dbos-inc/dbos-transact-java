plugins {
  scala
  id("java-library")
  alias(libs.plugins.maven.publish)
}

evaluationDependsOn(":transact")

val transactTestOutput = project(":transact").sourceSets.test.get().output

mavenPublishing {
  pom {
    name.set("DBOS Transact Scala Binding")
    description.set("Scala binding for the DBOS Transact Java SDK")
  }
}

dependencies {
  api(project(":transact"))
  api(libs.slf4j.api)
  compileOnly(libs.jspecify)

  implementation(libs.scala3.library)
  implementation(libs.scala.library)

  testImplementation(project(":transact"))
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)
  testRuntimeOnly(libs.logback.classic)
  testImplementation(libs.assertj.core)
  testImplementation(libs.postgresql)
  testImplementation(libs.hikaricp)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(libs.testcontainers.cockroachdb)
  testImplementation(transactTestOutput)
}

scala { zincVersion.set("1.10.1") }

tasks.withType<ScalaCompile> { scalaCompileOptions.additionalParameters?.add("-release:17") }

tasks.named("compileTestScala") { dependsOn(":transact:testClasses") }

val jar by
  tasks.getting(Jar::class) { manifest { attributes["Implementation-Title"] = project.name } }
