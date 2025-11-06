plugins {
    application
}

application {
    mainClass.set("dev.dbos.transact.cli.Main")
}

dependencies {
    implementation(project(":transact"))
}
