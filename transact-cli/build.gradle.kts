plugins {
    application
    id("pmd")
    id("com.diffplug.spotless") version "8.0.0"
    id("com.gradleup.shadow") version "9.2.2"
}

application {
    mainClass.set("dev.dbos.transact.cli.DBOSCommandLine")
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
    ruleSetFiles = files("${rootDir}/config/pmd/ruleset.xml")
    isConsoleOutput = true
    toolVersion = "7.16.0"
}

tasks.withType<Pmd> {
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

dependencies {
    implementation(project(":transact"))
    implementation("info.picocli:picocli:4.7.7")
}
