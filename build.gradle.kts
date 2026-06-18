plugins {
  id("dbos.base-conventions")
  alias(libs.plugins.spotless)
  alias(libs.plugins.versions)
  // Registers JVM attribute-matching rules (e.g. TargetJvmVersion compatibility) so the
  // formatterTools resolution below can select ktfmt's variants. Adds no source sets or tasks.
  `jvm-ecosystem`
}

allprojects {
  repositories {
    mavenCentral()
    gradlePluginPortal()
  }
}

// Spotless resolves the ktfmt formatter on its own internal configuration, which dependencyUpdates
// can't see. Parking it on a resolvable-only config here lets the update report flag new releases.
val formatterTools by configurations.creating {
  isCanBeConsumed = false
  isCanBeResolved = true
}

dependencies { formatterTools("com.facebook:ktfmt:${libs.versions.ktfmt.get()}") }

// The root project has no Java/Kotlin sources of its own; only format its own build scripts.
// Per-module formatting (java, kotlin, *.gradle.kts) lives in the dbos.quality-conventions plugin.
spotless {
  kotlinGradle {
    target("*.gradle.kts")
    ktfmt(libs.versions.ktfmt.get()).googleStyle()
    trimTrailingWhitespace()
    endWithNewline()
  }
}
