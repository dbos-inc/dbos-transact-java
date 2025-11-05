group = "dev.dbos"
version =  rootProject.extra["calculatedVersion"] as String

println("CLI version: $version") // prints when Gradle evaluates the build

plugins {
    application
}

application {
    mainClass.set("dev.dbos.transact.cli.Main")
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    implementation(project(":transact"))
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
