plugins {
  id("java-library")
  alias(libs.plugins.maven.publish)
}

mavenPublishing {
  pom {
    name.set("DBOS Transact jOOQ Step Factory")
    description.set("jOOQ step factory for DBOS Transact Java SDK")
  }
}

dependencies {
  api(project(":transact"))
  api(libs.jooq)
  compileOnly(libs.jaxb.api)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)

  testRuntimeOnly(libs.logback.classic)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(libs.postgresql)
  testImplementation(libs.hikaricp)
}
