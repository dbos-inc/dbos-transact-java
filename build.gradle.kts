plugins {
    id("pmd")
    id("com.diffplug.spotless") version "8.1.0"
    id("com.github.ben-manes.versions") version "0.53.0"
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
    extra["commitCount"] = "$commitCount"

    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "pmd")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.ben-manes.versions")

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

    tasks.withType<com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask> {
        rejectVersionIf {
            val isUnstable = listOf("alpha", "beta", "rc", "cr", "m", "preview", "b", "ea")
                .any { qualifier -> candidate.version.lowercase().contains(qualifier) }
            
            val isStable = listOf("release", "final", "ga")
                .any { qualifier -> currentVersion.lowercase().contains(qualifier) }

            isUnstable && !isStable
        }
    }

    plugins.withId("java") {
        // Force the published bytecode to be Java 17
        extensions.getByType<JavaPluginExtension>().apply {
            sourceCompatibility = JavaVersion.VERSION_17
            targetCompatibility = JavaVersion.VERSION_17
        }

        // Allow the compiler to see higher-version APIs for reflection but keep the bytecode target at 17.
        tasks.withType<JavaCompile> {
            options.release.set(17) 
        }

        // use the environment's JDK instead of the toolchain's JDK for tests
        tasks.withType<Test> {
            javaLauncher.set(null as JavaLauncher?) 
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