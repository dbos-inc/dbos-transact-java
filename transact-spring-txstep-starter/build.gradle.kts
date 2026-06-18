plugins {
  id("java-library")
  id("dbos.java-conventions")
  id("dbos.quality-conventions")
  id("dbos.publishing-conventions")
}

mavenPublishing {
  pom {
    name.set("DBOS Transact Spring Transactional Step Starter")
    description.set("Spring Boot auto-configuration for DBOS @TransactionalStep annotation")
  }
}

dependencies {
  api(project(":transact"))
  implementation(libs.slf4j.api)
  compileOnly(project(":transact-spring-boot-starter"))
  compileOnly(libs.spring.boot.autoconfigure)
  compileOnly(libs.spring.aop)
  compileOnly(libs.aspectjweaver)
  compileOnly(libs.spring.tx)
  compileOnly(libs.spring.jdbc)
  compileOnly(libs.spring.orm)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)

  testImplementation(project(":transact-spring-boot-starter"))
  testImplementation(libs.spring.boot.test)
  testImplementation(libs.spring.boot.autoconfigure)
  testImplementation(libs.spring.aop)
  testImplementation(libs.aspectjweaver)
  testImplementation(libs.spring.tx)
  testImplementation(libs.spring.jdbc)
  testImplementation(libs.spring.orm)
  testImplementation(libs.hibernate.core)
  testImplementation(libs.jdbi.core)
  testImplementation(libs.jdbi.spring)
  testCompileOnly(libs.jaxb.api)
  testImplementation(libs.jooq)
  testImplementation(libs.assertj.core)
  testImplementation(libs.testcontainers.postgresql)
  testImplementation(libs.postgresql)
  testImplementation(libs.hikaricp)
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
          implementation(project(":transact-spring-boot-starter"))
          implementation(platform(libs.spring.boot4.dependencies))
          implementation(platform(libs.junit.bom))
          implementation(libs.junit.jupiter)
          runtimeOnly(libs.junit.platform.launcher)
          implementation(libs.spring.boot4.test)
          implementation(libs.spring.boot4.autoconfigure)
          implementation("org.springframework:spring-aop")
          implementation(libs.aspectjweaver)
          implementation("org.springframework:spring-tx")
          implementation("org.springframework:spring-jdbc")
          implementation("org.springframework:spring-orm")
          implementation("org.hibernate.orm:hibernate-core")
          implementation(libs.jdbi.core)
          implementation(libs.jdbi.spring)
          compileOnly(libs.jaxb.api)
          implementation(libs.jooq)
          implementation(libs.assertj.core)
          implementation(libs.testcontainers.postgresql)
          implementation(libs.postgresql)
          implementation(libs.hikaricp)
          runtimeOnly(libs.logback.classic)
        }
      }
  }
}

tasks.named("test") { dependsOn("springBoot4Test") }
