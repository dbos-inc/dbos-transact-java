plugins {
  application
  alias(libs.plugins.shadow)
}

application { mainClass.set("dev.dbos.transact.cli.Main") }

dependencies {
  implementation(project(":transact"))
  implementation(libs.bundles.jackson)
  implementation(libs.picocli)
  runtimeOnly(libs.slf4j.simple)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)
  testImplementation(libs.testcontainers.postgresql)
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
