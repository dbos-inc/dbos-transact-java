plugins {
    id("pmd")
    id("com.diffplug.spotless") version "8.0.0"
}

allprojects {
    group = "dev.dbos"
    version = rootProject.extra["calculatedVersion"] as String

    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "pmd")
    apply(plugin = "com.diffplug.spotless")  // Spotless plugin

    // PMD configuration
    extensions.configure<org.gradle.api.plugins.quality.PmdExtension> {
        toolVersion = "7.16.0"
        ruleSets = listOf() // disable defaults
        ruleSetFiles = files("${rootDir}/config/pmd/ruleset.xml")
        isConsoleOutput = true
    }

    // Spotless configuration
    extensions.configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            googleJavaFormat()
            importOrder("dev.dbos", "java", "javax", "")
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
        }
    }

    plugins.withId("java") {
        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(17))
            }
        }

        tasks.named<Jar>("jar") {
            manifest {
                attributes["Implementation-Version"] = project.version
                attributes["Implementation-Title"] = project.name
                attributes["Implementation-Vendor"] = "DBOS, Inc"
                attributes["Implementation-Vendor-Id"] = project.group
                attributes["SCM-Revision"] = rootProject.extra["gitHash"] as String
            }
        }
    }
}