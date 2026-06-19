import org.graalvm.buildtools.gradle.dsl.GraalVMExtension

plugins {
  application
  alias(libs.plugins.shadow)
  alias(libs.plugins.graalvm.native)
  id("dbos.java-conventions")
  id("dbos.quality-conventions")
}

application { mainClass.set("dev.dbos.transact.cli.Main") }

dependencies {
  implementation(project(":transact"))
  implementation(libs.picocli)
  annotationProcessor(libs.picocli.codegen)
  runtimeOnly(libs.slf4j.simple)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)
  testImplementation(libs.testcontainers.postgresql)
}

tasks.named<JavaCompile>("compileJava") {
  options.compilerArgs.add("-Aproject=${project.group}/${project.name}")
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
  archiveBaseName.set("dbos")
  archiveVersion.set("")
  archiveClassifier.set("")
}

configure<GraalVMExtension> {
  metadataRepository { enabled.set(true) }

  binaries {
    named("main") {
      imageName.set("dbos")
      mainClass.set("dev.dbos.transact.cli.Main")
      buildArgs.add("--no-fallback")
      javaLauncher.set(
        javaToolchains.launcherFor {
          languageVersion.set(JavaLanguageVersion.of(21))
          vendor.set(JvmVendorSpec.GRAAL_VM)
        }
      )
    }
  }
}
