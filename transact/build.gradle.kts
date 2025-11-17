plugins {
    id("java")
    id("java-library")
    id("com.vanniktech.maven.publish") version "0.34.0"
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
    api("org.slf4j:slf4j-api:2.0.13") // logging api

    implementation("org.postgresql:postgresql:42.7.2")
    implementation("com.zaxxer:HikariCP:5.0.1") // Connection pool
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0") // json
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.0")
    implementation("com.cronutils:cron-utils:9.2.1") // cron for scheduled wf

    testImplementation(platform("org.junit:junit-bom:5.12.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit-pioneer:junit-pioneer:2.3.0")
    testImplementation("uk.org.webcompere:system-stubs-jupiter:2.1.8")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation("org.java-websocket:Java-WebSocket:1.5.6")
    testImplementation("ch.qos.logback:logback-classic:1.5.6")
    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation("io.rest-assured:rest-assured:5.4.0")
    testImplementation("io.rest-assured:json-path:5.4.0")
    testImplementation("io.rest-assured:xml-path:5.4.0")
    testImplementation("org.testcontainers:postgresql:1.21.3")
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

val publishingToMavenCentral = gradle.startParameter.taskNames.any { it.contains("publishToMavenCentral") }

mavenPublishing {
    publishToMavenCentral(automaticRelease = true)
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