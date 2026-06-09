plugins {
  id("java-platform")
  alias(libs.plugins.maven.publish)
}

javaPlatform { allowDependencies() }

dependencies {
  constraints {
    api(project(":transact"))
    api(project(":transact-cli"))
    api(project(":transact-spring-boot-starter"))
    api(project(":transact-spring-txstep-starter"))
    api(project(":transact-jdbi-step-factory"))
    api(project(":transact-jooq-step-factory"))
  }
}

mavenPublishing {
  pom {
    name.set("DBOS Transact BOM")
    description.set("Bill of Materials for DBOS Transact Java SDK")
    packaging = "pom"
  }
}
