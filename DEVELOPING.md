# dbos-transact-java
DBOS Transact Java SDK

## Setting up dev environment

Install a recent OpenJDK. I use OpenJDK 21.    
https://adoptium.net/en-GB/temurin/releases/?os=any&arch=any&version=21

Recommended IDE IntelliJ (Community edition is fine).
But feel free to use vi or VSCode, if you are more comfortable with it.

Postgres docker container with
localhost   
port 5432   
user postgres

export PGPASSWORD = password for postgres user

## full build

```shell
./gradlew clean build
```

## formatting

The DBOS Transact Java SDK uses [Spotless](https://github.com/diffplug/spotless) for consistent formatting.
Spotless checks formatting automatically during build
If build fails because of format violations, you can update the code with the `spotlessApply` Gradle task.

```shell
./gradlew spotlessApply
```

## run tests

```shell
./gradlew clean test
```

## publish to local maven repository

```shell
./gradlew publishToMavenLocal
```

## import transact into your application

Add to your build.gradle.kts

```kotlin
implementation("dev.dbos:transact:1.0-SNAPSHOT")      
implementation("ch.qos.logback:logback-classic:1.5.6")
```

Annotations @Workflow, @Transaction, @Step need to be on implementation class methods. 
