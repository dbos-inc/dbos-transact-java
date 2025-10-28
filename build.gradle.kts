fun Project.runCommand(vararg args: String): String {
    val process = ProcessBuilder(*args)
        .directory(projectDir)
        .redirectErrorStream(true)
        .start()
    
    val output = process.inputStream.bufferedReader().readText()
    val exitCode = process.waitFor()
    
    if (exitCode != 0) {
        throw GradleException("Command failed with exit code $exitCode: ${args.joinToString(" ")}")
    }
    
    return output.trim()
}

val gitHash: String by lazy {
    runCommand("git", "rev-parse", "--short", "HEAD")
}

val gitTag: String? by lazy {
    runCatching {
        runCommand("git", "describe", "--abbrev=0", "--tags")
    }.getOrNull()
}

val commitCount: String by lazy {
    val range = if (gitTag.isNullOrEmpty()) "HEAD" else "$gitTag..HEAD"
    runCommand("git", "rev-list", "--count", range)
}

val branch: String by lazy {
    // First, try GitHub Actions environment variable
    val githubBranch = System.getenv("GITHUB_REF_NAME")
    if (!githubBranch.isNullOrBlank()) githubBranch
    
    // Fallback to local git command
    else {
        runCommand("git", "rev-parse", "--abbrev-ref", "HEAD")
    }
}

fun parseTag(tag: String): Triple<Int, Int, Int>? {
    val regex = Regex("""v?(\d+)\.(\d+)\.(\d+)""")
    val match = regex.matchEntire(tag.trim()) ?: return null
    val (major, minor, patch) = match.destructured
    return Triple(major.toInt(), minor.toInt(), patch.toInt())
}

fun calcVersion(): String {
    var (major, minor, patch) = parseTag(gitTag ?: "") ?: Triple(0, 1, 0)

    if (branch == "main") {
        return "$major.${minor + 1}.$patch-m$commitCount"
    }

    if (branch.startsWith("release/v")) {
        return "$major.$minor.$patch"
    }

    return "$major.${minor + 1}.$patch-a$commitCount-g$gitHash"
}

version = calcVersion()
println("Project version: $version") // prints when Gradle evaluates the build

tasks.register("printGitStatus") {
    doLast {
        val status = project.runCommand("git", "status", "--porcelain")
        println("Git status:\n$status")
    }
}

plugins {
    id("java")
    id("java-library")
    id("maven-publish")
    id("pmd")
    id("com.diffplug.spotless") version "8.0.0"
}

group = "dev.dbos"

java {
    withSourcesJar()
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

spotless {
    java {
        googleJavaFormat()
        importOrder("dev.dbos", "java", "javax", "")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
}

pmd {
    ruleSets = listOf() // disable defaults
    ruleSetFiles = files("config/pmd/ruleset.xml")
    isConsoleOutput = true
    toolVersion = "7.16.0"
}

tasks.withType<Pmd> {
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    api("org.slf4j:slf4j-api:2.0.13") // logging api

    implementation("org.postgresql:postgresql:42.7.2")
    implementation("com.zaxxer:HikariCP:5.0.1") // Connection pool
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0") // json
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.0")
    implementation("com.cronutils:cron-utils:9.2.1") // cron for scheduled wf

    testImplementation("ch.qos.logback:logback-classic:1.5.6")
    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation("io.rest-assured:rest-assured:5.4.0")
    testImplementation("io.rest-assured:json-path:5.4.0")
    testImplementation("io.rest-assured:xml-path:5.4.0")
    testImplementation(platform("org.junit:junit-bom:5.12.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.java-websocket:Java-WebSocket:1.5.6")
    testImplementation("org.junit-pioneer:junit-pioneer:2.3.0")
    testImplementation("uk.org.webcompere:system-stubs-jupiter:2.1.8")

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
    environment("DBOS_SYSTEM_DATABASE_URL", "jdbc:postgresql://localhost:5432/dbos_java_sys")
}

tasks.jar {
    archiveBaseName.set("transact")
    // Will produce: build/libs/transact-1.0.0.jar

    manifest {
        attributes["Implementation-Version"] = project.version.toString()
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = "transact"
            groupId = project.group.toString()
            version = project.version.toString()
        }
    }
    repositories {
        mavenLocal()
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/dbos-inc/dbos-transact-java") // replace OWNER/REPO
            credentials {
                username = project.findProperty("gpr.user")?.toString()
                    ?: System.getenv("USERNAME_MAVEN")
                password = project.findProperty("gpr.token")?.toString()
                    ?: System.getenv("TOKEN_MAVEN")
            }
        }
    }
}
