group = "dev.dbos"
version = rootProject.extra["calculatedVersion"] as String

plugins {
    application
    id("com.gradleup.shadow") version "9.2.2"
}

application {
    mainClass.set("dev.dbos.transact.cli.DBOSCommandLine")
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation(project(":transact"))
    implementation("info.picocli:picocli:4.7.7")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

tasks.jar {
    manifest {
        attributes["Implementation-Version"] = project.version
        attributes["Implementation-Title"] = project.name
        attributes["Implementation-Vendor"] = "DBOS, Inc"
        attributes["Implementation-Vendor-Id"] = project.group
        attributes["SCM-Revision"] = rootProject.extra["gitHash"] as String
    }
}
