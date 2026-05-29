plugins {
  id("java-library")
  alias(libs.plugins.maven.publish)
}

mavenPublishing {
  pom {
    name.set("DBOS Transact Spring Boot Starter")
    description.set("Spring Boot auto-configuration for DBOS Transact Java SDK")
  }
}

dependencies {
  api(project(":transact"))
  compileOnly(libs.spring.boot.autoconfigure)
  compileOnly(libs.spring.aop)
  compileOnly(libs.aspectjweaver)
  annotationProcessor(libs.spring.boot.configuration.processor)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)

  testImplementation(libs.spring.boot.test)
  testImplementation(libs.assertj.core)
  testImplementation(libs.spring.boot.autoconfigure)
  testImplementation(libs.spring.aop)
  testImplementation(libs.aspectjweaver)
  testImplementation(libs.mockito.core)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(libs.postgresql)
  testImplementation(libs.hikaricp)
  testImplementation(libs.sqlite.jdbc)
  testRuntimeOnly(libs.logback.classic)
}

testing {
  suites {
    val springBoot4Test by
      registering(JvmTestSuite::class) {
        sources {
          java { setSrcDirs(sourceSets["test"].java.srcDirs) }
          resources { setSrcDirs(sourceSets["test"].resources.srcDirs) }
        }
        dependencies {
          implementation(project())
          implementation(platform(libs.spring.boot4.dependencies))
          implementation(platform(libs.junit.bom))
          implementation(libs.junit.jupiter)
          runtimeOnly(libs.junit.platform.launcher)
          implementation(libs.spring.boot4.test)
          implementation(libs.assertj.core)
          implementation(libs.spring.boot4.autoconfigure)
          implementation("org.springframework:spring-aop")
          implementation(libs.aspectjweaver)
          implementation(libs.mockito.core)
          implementation(libs.testcontainers.postgresql)
          implementation(libs.postgresql)
          implementation(libs.hikaricp)
          implementation(libs.sqlite.jdbc)
          runtimeOnly(libs.logback.classic)
        }
      }
  }
}

tasks.named("test") { dependsOn("springBoot4Test") }
