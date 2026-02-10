import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.vanniktech.maven.publish.DeploymentValidation

plugins {
    id("java")
    id("java-library")
    kotlin("jvm") version "2.3.0"
    id("com.vanniktech.maven.publish") version "0.36.0"
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-Xlint:unchecked")    // warn about unchecked operations
    options.compilerArgs.add("-Xlint:deprecation")  // warn about deprecated APIs
    options.compilerArgs.add("-Xlint:rawtypes")     // warn about raw types
    options.compilerArgs.add("-Werror")             // treat all warnings as errors
}

tasks.withType<Javadoc> {
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:all,-missing", "-quiet")  // hide warnings for missing javadoc comments
        encoding = "UTF-8"                                  // optional, ensures UTF-8 for docs
    }
}

tasks.named("build") {
    dependsOn("javadoc")
}

dependencies {
    api("org.slf4j:slf4j-api:2.0.17") // logging api

    implementation("org.postgresql:postgresql:42.7.9")
    implementation("com.zaxxer:HikariCP:7.0.2") // Connection pool
    implementation("com.fasterxml.jackson.core:jackson-databind:2.20.1") // json
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.20.1")
    implementation("com.cronutils:cron-utils:9.2.1") // cron for scheduled wf
    implementation("io.netty:netty-all:4.1.130.Final") // netty for websocket

    compileOnly("org.jspecify:jspecify:1.0.0")

    testImplementation(platform("org.junit:junit-bom:6.0.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit-pioneer:junit-pioneer:2.3.0")
    testImplementation("uk.org.webcompere:system-stubs-jupiter:2.1.8")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation("org.java-websocket:Java-WebSocket:1.6.0")
    testImplementation("ch.qos.logback:logback-classic:1.5.24")
    testImplementation("org.mockito:mockito-core:5.21.0")
    testImplementation("io.rest-assured:rest-assured:6.0.0")
    testImplementation("io.rest-assured:json-path:6.0.0")
    testImplementation("io.rest-assured:xml-path:6.0.0")
    testImplementation("org.apache.maven:maven-artifact:3.9.12")
}

val projectVersion = project.version.toString()

tasks.processResources {
    inputs.property("version", projectVersion)

    filesMatching("**/app.properties") {
        expand(mapOf("projectVersion" to projectVersion))
    }
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

tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        // jvmTarget now uses the JvmTarget enum instead of a String
        jvmTarget.set(JvmTarget.JVM_17)
        
        // freeCompilerArgs is now a Property/ListProperty, so we use .add() or .addAll()
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

val publishingToMavenCentral = gradle.startParameter.taskNames.any { it.contains("publishToMavenCentral") }

mavenPublishing {
    publishToMavenCentral(automaticRelease = true, validateDeployment = DeploymentValidation.NONE)
    if (publishingToMavenCentral) {
        signAllPublications()
    }

    pom {
        name.set("DBOS Transact")
        description.set("DBOS Transact Java SDK for lightweight durable workflows")
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