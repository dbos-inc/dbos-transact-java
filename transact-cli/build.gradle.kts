plugins {
    application
    id("com.gradleup.shadow") version "9.2.2"
}

application {
    mainClass.set("dev.dbos.transact.cli.Main")
}

dependencies {
    implementation(project(":transact"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0") // json
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.0")
    implementation("info.picocli:picocli:4.7.7")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.13")

    testImplementation(platform("org.junit:junit-bom:5.12.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true

        afterSuite(KotlinClosure2({ desc: TestDescriptor, result: TestResult ->
            if (desc.parent == null) {
                println("\nTest Results:")
                println("  Tests run: ${result.testCount}")
                println("  Passed: ${result.successfulTestCount}")
                println("  Failed: ${result.failedTestCount}")
                println("  Skipped: ${result.skippedTestCount}")
            }
        }))
    }
}

val prepareLaunchers by tasks.registering {
    val outputDir = layout.buildDirectory.dir("dist/bin").get().asFile
    doLast {
        outputDir.mkdirs()

        // Linux/macOS launcher
        val linuxLauncher = File(outputDir, "dbos")
        linuxLauncher.writeText(
            "#!/bin/bash\n" +
            "DIR=\"\$(cd \"\$(dirname \"\\\${BASH_SOURCE[0]}\\\")\" && pwd)\"\n" +
            "java -jar \"\$DIR/../lib/transact-cli-${'$'}{project.version}-all.jar\" \"\$@\"\n"
        )
        linuxLauncher.setExecutable(true)

        // Windows launcher
        val windowsLauncher = File(outputDir, "dbos.bat")
        windowsLauncher.writeText("""
            @echo off
            set DIR=%~dp0
            java -jar "%DIR%..\lib\transact-cli-${'$'}{project.version}-all.jar" %*
        """.trimIndent())
    }
}

val shadowJarTask = tasks.named("shadowJar")

val packageZip by tasks.registering(Zip::class) {
    dependsOn(shadowJarTask, prepareLaunchers)

    archiveBaseName.set("dbos")
    archiveVersion.set(project.version.toString())
    destinationDirectory.set(layout.buildDirectory.dir("dist"))

    // Launcher scripts
    from(layout.buildDirectory.dir("dist/bin")) { into("bin") }

    // Fat JAR from com.gradleup.shadow
    from(shadowJarTask.map { it.outputs.files.singleFile }) { into("lib") }

    // README
    from(layout.projectDirectory.file("README.md")) { into(".") }
}
 