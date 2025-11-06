plugins {
    application
    id("com.gradleup.shadow") version "9.2.2"
}

application {
    mainClass.set("dev.dbos.transact.cli.DBOSCommandLine")
}

dependencies {
    implementation(project(":transact"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0") // json
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.0")
    implementation("info.picocli:picocli:4.7.7")
}
