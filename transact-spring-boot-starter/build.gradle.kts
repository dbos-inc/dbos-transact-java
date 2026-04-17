import com.vanniktech.maven.publish.DeploymentValidation

plugins {
  id("java-library")
  id("com.vanniktech.maven.publish")
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
  compileOnly("org.springframework.boot:spring-boot-autoconfigure:3.4.4")
  compileOnly("org.springframework:spring-aop:6.2.5")
  compileOnly("org.aspectj:aspectjweaver:1.9.22.1")
  annotationProcessor("org.springframework.boot:spring-boot-configuration-processor:3.4.4")

  testImplementation(platform("org.junit:junit-bom:6.0.3"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")

  testImplementation("org.springframework.boot:spring-boot-test:3.4.4")
  testImplementation("org.assertj:assertj-core:3.27.3")
  testImplementation("org.springframework.boot:spring-boot-autoconfigure:3.4.4")
  testImplementation("org.springframework:spring-aop:6.2.5")
  testImplementation("org.aspectj:aspectjweaver:1.9.22.1")
  testImplementation("org.mockito:mockito-core:5.22.0")
  testRuntimeOnly("ch.qos.logback:logback-classic:1.5.32")
}

val publishingToMavenCentral =
  gradle.startParameter.taskNames.any { it.contains("publishToMavenCentral") }

mavenPublishing {
  publishToMavenCentral(automaticRelease = true, validateDeployment = DeploymentValidation.NONE)
  if (publishingToMavenCentral) {
    signAllPublications()
  }

  pom {
    name.set("DBOS Transact Spring Boot Starter")
    description.set("Spring Boot auto-configuration for DBOS Transact Java SDK")
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
