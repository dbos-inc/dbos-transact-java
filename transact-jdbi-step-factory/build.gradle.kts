plugins { id("java-library") }

tasks.withType<JavaCompile> {
  options.compilerArgs.add("-Xlint:unchecked")
  options.compilerArgs.add("-Xlint:deprecation")
  options.compilerArgs.add("-Xlint:rawtypes")
  options.compilerArgs.add("-Werror")
}

dependencies {
  api(project(":transact"))
  api(libs.jdbi.core)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)

  testRuntimeOnly(libs.logback.classic)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(libs.postgresql)
  testImplementation(libs.hikaricp)
}
