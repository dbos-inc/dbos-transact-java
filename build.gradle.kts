plugins {
    id("pmd")
    id("com.diffplug.spotless") version "8.0.0"
}

fun runCommand(vararg args: String): String {
    val process = ProcessBuilder(*args)
        .directory(rootDir)
        .redirectErrorStream(true)
        .start()
    
    val output = process.inputStream.bufferedReader().readText()
    val exitCode = process.waitFor()
    
    if (exitCode != 0) {
        throw GradleException("Command failed with exit code $exitCode: ${args.joinToString(" ")}")
    }
    
    return output.trim()
}

val gitHash: String by lazy {
    runCommand("git", "rev-parse", "--short", "HEAD")
}

val gitTag: String? by lazy {
    runCatching {
        runCommand("git", "describe", "--abbrev=0", "--tags")
    }.getOrNull()
}

val commitCount: Int by lazy {
    val range = if (gitTag.isNullOrEmpty()) "HEAD" else "$gitTag..HEAD"
    runCommand("git", "rev-list", "--count", range).toInt()
}

val branch: String by lazy {
    // First, try GitHub Actions environment variable
    val githubBranch = System.getenv("GITHUB_REF_NAME")
    if (!githubBranch.isNullOrBlank()) githubBranch
    
    // Fallback to local git command
    else {
        runCommand("git", "rev-parse", "--abbrev-ref", "HEAD")
    }
}

fun parseTag(tag: String): Triple<Int, Int, Int>? {
    val regex = Regex("""v?(\d+)\.(\d+)\.(\d+)""")
    val match = regex.matchEntire(tag.trim()) ?: return null
    val (major, minor, patch) = match.destructured
    return Triple(major.toInt(), minor.toInt(), patch.toInt())
}

fun calcVersion(): String {
    var (major, minor, patch) = parseTag(gitTag ?: "") ?: Triple(0, 1, 0)

    if (branch == "main") {
        return "$major.${minor + 1}.$patch-m$commitCount"
    }

    if (branch.startsWith("release/v")) {
        if (commitCount == 0) {
            return "$major.$minor.$patch"
        } else {
            return "$major.$minor.${patch + 1}-rc$commitCount"
        }
    }

    return "$major.${minor + 1}.$patch-a$commitCount-g$gitHash"
}

val calculatedVersion: String by lazy {
    calcVersion()
}

// prints when Gradle evaluates the build
println("DBOS Transact version: $calculatedVersion") 

allprojects {
    group = "dev.dbos"
    version = calculatedVersion

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
                attributes["SCM-Revision"] = gitHash
            }
        }
    }
}