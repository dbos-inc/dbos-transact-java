import com.vanniktech.maven.publish.DeploymentValidation

plugins {
  id("java-library")
  alias(libs.plugins.maven.publish)
}

tasks.withType<JavaCompile> {
  options.compilerArgs.add("-Xlint:unchecked")
  options.compilerArgs.add("-Xlint:deprecation")
  options.compilerArgs.add("-Xlint:rawtypes")
  options.compilerArgs.add("-Werror")
}

tasks.withType<Javadoc> {
  (options as StandardJavadocDocletOptions).apply {
    addStringOption("Xdoclint:all,-missing", "-quiet")
    encoding = "UTF-8"
  }
}

tasks.named("build") { dependsOn("javadoc") }

dependencies {
  api(project(":transact"))
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

val publishingToMavenCentral =
  gradle.startParameter.taskNames.any { it.contains("publishToMavenCentral") }

mavenPublishing {
  publishToMavenCentral(automaticRelease = true, validateDeployment = DeploymentValidation.NONE)
  if (publishingToMavenCentral) {
    signAllPublications()
  }

  pom {
    name.set("DBOS Transact Spring Transactional Step Starter")
    description.set("Spring Boot auto-configuration for DBOS @TransactionalStep annotation")
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
