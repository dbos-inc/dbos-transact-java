# dbos-transact-java
DBOS Transact Java SDK 

## Setting up dev environment

Install a recent OpenJDK. I use OpenJDK 21.    
https://adoptium.net/en-GB/temurin/releases/?os=any&arch=any&version=21

Recommended IDE IntelliJ (Community edition is fine).
But feel free to use vi, if you are more comfortable with it.  

Postgres docker container with
localhost   
port 5432   
user postgres

export PGPASSWORD = password for postgres user  

## build

./gradlew clean build

## run tests

./gradlew clean test

## publish to local maven repository

./gradlew publishToMavenLocal

## import transact into your application

Add to your build.gradle.kts

implementation("dev.dbos:transact:1.0-SNAPSHOT")      
implementation("ch.qos.logback:logback-classic:1.5.6")   

Annotations @Workflow, @Transaction, @Step need to be on implementation class methods. 




