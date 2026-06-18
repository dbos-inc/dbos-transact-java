plugins {
  id("java-library")
  id("dbos.java-conventions")
  id("dbos.quality-conventions")
  id("dbos.publishing-conventions")
}

mavenPublishing {
  pom {
    name.set("DBOS Transact JDBI Step Factory")
    description.set("JDBI step factory for DBOS Transact Java SDK")
  }
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
