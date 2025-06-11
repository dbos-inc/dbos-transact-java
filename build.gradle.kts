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
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
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