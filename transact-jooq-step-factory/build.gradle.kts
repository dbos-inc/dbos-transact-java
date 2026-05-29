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

val publishingToMavenCentral =
  gradle.startParameter.taskNames.any { it.contains("publishToMavenCentral") }

mavenPublishing {
  publishToMavenCentral(automaticRelease = true, validateDeployment = DeploymentValidation.NONE)
  if (publishingToMavenCentral) {
    signAllPublications()
  }

  pom {
    name.set("DBOS Transact jOOQ Step Factory")
    description.set("jOOQ step factory for DBOS Transact Java SDK")
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
