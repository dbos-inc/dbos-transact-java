plugins {
    id("java")
    id("java-library")
    id("maven-publish")

}

group = "dev.dbos"
version = "1.0-SNAPSHOT"


tasks.withType<JavaCompile> {
    sourceCompatibility = "11"
    targetCompatibility = "11"
}

repositories {
    mavenCentral()
}

dependencies {
    api("org.slf4j:slf4j-api:2.0.13") // logging api

    implementation("org.flywaydb:flyway-core:9.22.0") // migration
    implementation("org.postgresql:postgresql:42.6.0")
    implementation("com.zaxxer:HikariCP:5.0.1") // Connection pool
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0") // json
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.0")
    implementation("com.cronutils:cron-utils:9.2.1") // cron for scheduled wf

    testImplementation("ch.qos.logback:logback-classic:1.5.6")
    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
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

tasks.jar {
    archiveBaseName.set("transact")
    // Will produce: build/libs/transact-1.0.0.jar
}

publishing {

    publications {
        create<MavenPublication>("mavenJava") {

            // change in
            artifactId = "transact"

            from(components["java"])

        }
    }
    repositories {
        mavenLocal()
    }
}