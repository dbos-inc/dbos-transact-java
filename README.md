# dbos-transact-java
DBOS Transact Java SDK 

## setup

For tests to run setup a postgres docker container running on   
localhost   
port 5432   
user postgres   
export PGPASSWORD=<password>

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


