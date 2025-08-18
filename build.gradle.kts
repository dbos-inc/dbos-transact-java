import java.io.ByteArrayOutputStream

// Get the short Git hash
val gitHash: String by lazy {
    ByteArrayOutputStream().also { stdout ->
        exec {
            commandLine = listOf("git", "rev-parse", "--short", "HEAD")
            standardOutput = stdout
        }
    }.toString().trim()
}

// Get the commit count
val commitCount: String by lazy {
    ByteArrayOutputStream().also { stdout ->
        exec {
            commandLine = listOf("git", "rev-list", "--count", "HEAD")
            standardOutput = stdout
        }
    }.toString().trim()
}

// Get the current branch name
val branchName: String by lazy {
    ByteArrayOutputStream().also { stdout ->
        exec {
            commandLine = listOf("git", "rev-parse", "--abbrev-ref", "HEAD")
            standardOutput = stdout
        }
    }.toString().trim()
}

// Note, this versioning scheme is fine for preview releases
// but we'll want something more robust once we want to bump
// the major or minor version number
val baseVersion = System.getenv("BASE_VERSION") ?: "0.5"
val safeBranchName = if (branchName == "main" || branchName == "HEAD") "" else ".${branchName.replace("/", "-")}"
version = "$baseVersion.$commitCount+g$gitHash$safeBranchName"

println("Project version: $version") // prints when Gradle evaluates the build

plugins {
    id("java")
    id("java-library")
    id("maven-publish")
    id("com.diffplug.spotless") version "6.25.0"
}

group = "dev.dbos"

tasks.withType<JavaCompile> {
    options.release.set(11) // Targets Java 11 bytecode (RECOMMENDED)
    // (Alternative: sourceCompatibility = "11"; targetCompatibility = "11")
}

spotless {
    java {
        eclipse().configFile("config/eclipse-code-formatter.xml")
        importOrder("dev.dbos", "java", "javax", "")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
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

    // http + jersey
    implementation("org.apache.tomcat.embed:tomcat-embed-core:10.1.12")
    implementation("org.apache.tomcat.embed:tomcat-embed-jasper:10.1.12")
    implementation("jakarta.ws.rs:jakarta.ws.rs-api:3.1.0")
    implementation("jakarta.servlet:jakarta.servlet-api:6.0.0")
    implementation("org.glassfish.jersey.containers:jersey-container-servlet-core:3.1.0")
    implementation("org.glassfish.jersey.inject:jersey-hk2:3.1.1")
    implementation("org.glassfish.jersey.core:jersey-server:3.1.0")
    implementation("org.glassfish.jersey.media:jersey-media-json-jackson:3.1.0")

    testImplementation("ch.qos.logback:logback-classic:1.5.6")
    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation("io.rest-assured:rest-assured:5.4.0")
    testImplementation("io.rest-assured:json-path:5.4.0")
    testImplementation("io.rest-assured:xml-path:5.4.0")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.java-websocket:Java-WebSocket:1.5.6")
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
                    ?: System.getenv("GH_MAVEN_USERNAME")
                password = project.findProperty("gpr.token")?.toString()
                    ?: System.getenv("GH_MAVEN_TOKEN")
            }
        }
    }
}
