rootProject.name = "dbos-transact-java"

include(
  "transact",
  "transact-bom",
  "transact-cli",
  "transact-spring-boot-starter",
  "transact-spring-txstep-starter",
  "transact-jdbi-step-factory",
  "transact-jooq-step-factory",
)

plugins { id("org.gradle.toolchains.foojay-resolver") version "1.0.0" }

toolchainManagement {
  jvm {
    javaRepositories {
      repository("foojay") {
        resolverClass.set(org.gradle.toolchains.foojay.FoojayToolchainResolver::class.java)
      }
    }
  }
}
