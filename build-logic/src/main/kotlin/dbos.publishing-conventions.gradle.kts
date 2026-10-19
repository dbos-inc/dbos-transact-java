import com.vanniktech.maven.publish.DeploymentValidation

plugins { id("com.vanniktech.maven.publish") }

val publishingToMavenCentral =
  gradle.startParameter.taskNames.any { it.contains("publishToMavenCentral") }

tasks.withType<Javadoc> {
  (options as StandardJavadocDocletOptions).apply {
    addStringOption("Xdoclint:all,-missing", "-quiet")
    encoding = "UTF-8"
  }
}

tasks.named("build") { dependsOn("javadoc") }

// Every main and release/* build goes to GitHub Packages; only full releases also go to Maven
// Central. Credentials come from the GitHubPackagesUsername and GitHubPackagesPassword properties.
extensions.configure<PublishingExtension> {
  repositories {
    maven {
      name = "GitHubPackages"
      url = uri("https://maven.pkg.github.com/dbos-inc/dbos-transact-java")
      credentials(PasswordCredentials::class)
    }
  }
}

extensions.configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
  publishToMavenCentral(automaticRelease = true, validateDeployment = DeploymentValidation.NONE)
  if (publishingToMavenCentral) signAllPublications()

  pom {
    inceptionYear.set("2025")
    url.set("https://github.com/dbos-inc/dbos-transact-java")

    licenses {
      license {
        name.set("MIT License")
        url.set("https://opensource.org/licenses/MIT")
      }
    }

    developers {
      developer {
        id.set("dbos-inc")
        name.set("DBOS Inc")
        email.set("support@dbos.dev")
      }
    }

    scm {
      connection.set("scm:git:git://github.com/dbos-inc/dbos-transact-java.git")
      developerConnection.set("scm:git:ssh://github.com:dbos-inc/dbos-transact-java.git")
      url.set("https://github.com/dbos-inc/dbos-transact-java/tree/main")
    }
  }
}
