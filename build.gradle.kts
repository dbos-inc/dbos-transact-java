plugins {
    id("java")
    id("java-library")
    id("maven-publish")

}

group = "dev.dbos"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    api("org.slf4j:slf4j-api:2.0.13")

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