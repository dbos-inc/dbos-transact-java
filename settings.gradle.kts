rootProject.name = "transact"
include("transact", "transact-cli")

plugins {
    id("org.gradle.toolchains.foojay-resolver") version "1.0.0"
}

toolchainManagement {
    jvm {
        javaRepositories {
            repository("foojay") {
                resolverClass.set(org.gradle.toolchains.foojay.FoojayToolchainResolver::class.java)
            }
        }
    }
}

fun runCommand(vararg args: String): String {
    val process = ProcessBuilder(*args)
        .directory(settingsDir)
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

gradle.rootProject {
    extra["calculatedVersion"] = calcVersion()
    extra["gitHash"] = gitHash
}