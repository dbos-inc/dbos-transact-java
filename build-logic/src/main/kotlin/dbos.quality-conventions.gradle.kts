import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask

plugins {
  pmd
  id("com.diffplug.spotless")
  id("com.github.ben-manes.versions")
}

val libs = the<org.gradle.accessors.dm.LibrariesForLibs>()

extensions.configure<org.gradle.api.plugins.quality.PmdExtension> {
  toolVersion = libs.versions.pmd.get()
  ruleSets = listOf() // disable defaults
  ruleSetFiles = files("${rootDir}/config/pmd/ruleset.xml")
  isConsoleOutput = true
}

extensions.configure<com.diffplug.gradle.spotless.SpotlessExtension> {
  java {
    googleJavaFormat()
    cleanthat()
    formatAnnotations()
    removeUnusedImports()
    importOrder("dev.dbos", "java", "javax", "")
    trimTrailingWhitespace()
    endWithNewline()
  }
  kotlin {
    target("**/*.kt")
    targetExclude("build/**/*.kt")
    ktfmt(libs.versions.ktfmt.get()).googleStyle()
    trimTrailingWhitespace()
    endWithNewline()
  }
  kotlinGradle {
    target("**/*.gradle.kts")
    ktfmt(libs.versions.ktfmt.get()).googleStyle()
    trimTrailingWhitespace()
    endWithNewline()
  }
}

tasks.withType<DependencyUpdatesTask> {
  rejectVersionIf {
    fun isUnstable(version: String) =
      listOf("alpha", "beta", "rc", "cr", "m", "preview", "b", "ea").any { qualifier ->
        version.lowercase().contains(qualifier)
      }
    isUnstable(candidate.version) && !isUnstable(currentVersion)
  }
}
