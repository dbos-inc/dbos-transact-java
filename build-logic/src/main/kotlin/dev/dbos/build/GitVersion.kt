package dev.dbos.build

import java.io.File

/**
 * Derives the project version and short commit hash from git. Results are memoized so the git
 * subprocesses run at most once per build, regardless of how many modules ask.
 */
object GitVersion {
  @Volatile private var cachedVersion: String? = null
  @Volatile private var cachedHash: String? = null

  fun gitHash(root: File): String =
    cachedHash ?: run(root, "git", "rev-parse", "--short", "HEAD").also { cachedHash = it }

  fun version(root: File): String =
    cachedVersion ?: calcVersion(root).also { cachedVersion = it }

  private fun gitTag(root: File): String? =
    runCatching { run(root, "git", "describe", "--abbrev=0", "--tags") }.getOrNull()

  private fun commitCount(root: File): Int {
    val tag = gitTag(root)
    val range = if (tag.isNullOrEmpty()) "HEAD" else "$tag..HEAD"
    return run(root, "git", "rev-list", "--count", range).toInt()
  }

  private fun branch(root: File): String {
    // First, try the GitHub Actions environment variable, then fall back to git.
    val githubBranch = System.getenv("GITHUB_REF_NAME")
    return if (!githubBranch.isNullOrBlank()) githubBranch
    else run(root, "git", "rev-parse", "--abbrev-ref", "HEAD")
  }

  private fun parseTag(tag: String): Triple<Int, Int, Int>? {
    val match = Regex("""v?(\d+)\.(\d+)\.(\d+)""").matchEntire(tag.trim()) ?: return null
    val (major, minor, patch) = match.destructured
    return Triple(major.toInt(), minor.toInt(), patch.toInt())
  }

  private fun calcVersion(root: File): String {
    val (major, minor, patch) = parseTag(gitTag(root) ?: "") ?: Triple(0, 1, 0)
    val branch = branch(root)
    val commitCount = commitCount(root)

    return when {
      branch == "main" -> "$major.${minor + 1}.$patch-m$commitCount"
      branch.startsWith("release/") ->
        if (commitCount == 0) "$major.$minor.$patch" else "$major.$minor.${patch + 1}-rc$commitCount"
      else -> "$major.${minor + 1}.$patch-a$commitCount-g${gitHash(root)}"
    }
  }

  private fun run(root: File, vararg args: String): String {
    val process =
      ProcessBuilder(*args).directory(root).redirectErrorStream(true).start()
    val output = process.inputStream.bufferedReader().readText()
    val exitCode = process.waitFor()
    if (exitCode != 0) {
      throw RuntimeException(
        "Command failed with exit code $exitCode: ${args.joinToString(" ")}\n$output")
    }
    return output.trim()
  }
}
