plugins {
    application
    id("com.gradleup.shadow") version "9.2.2"
}

application {
    mainClass.set("dev.dbos.transact.cli.DBOSCommandLine")
}

dependencies {
    implementation(project(":transact"))
    implementation("info.picocli:picocli:4.7.7")
}
