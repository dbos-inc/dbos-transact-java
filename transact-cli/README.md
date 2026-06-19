# DBOS CLI

The DBOS CLI is a command-line interface for managing the DBOS system database.

## Installation

DBOS CLI is distributed in two forms:

* A native executable (`dbos`) that runs directly without a JVM. This is the
  recommended way to run the CLI.

  ```shell
  $ dbos --version
  dbos v0.10.0
  ```

* A JAR with dependencies (also known as a fat or uber JAR), run with the
  `-jar` option of the [`java` command](https://docs.oracle.com/en/java/javase/17/docs/specs/man/java.html).

  ```shell
  $ java -jar dbos.jar --version
  dbos v0.10.0
  ```

The examples below use the native `dbos` executable; substitute
`java -jar dbos.jar` if you are using the JAR.

### Building the native executable

The native executable is built with [GraalVM Native Image](https://www.graalvm.org/latest/reference-manual/native-image/)
via the [Gradle native-build-tools plugin](https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html):

```shell
./gradlew :transact-cli:nativeCompile
```

The build downloads a GraalVM toolchain automatically (via the foojay toolchain
resolver) and writes the executable to
`transact-cli/build/native/nativeCompile/dbos`. Native Image requires a local C
toolchain (`gcc`/`clang`, `glibc`/`zlib` development headers) on the build machine.

## Configuration

### Database Connection Configuration

Many of the DBOS CLI commands require a DBOS system database connection to operate.
The DBOS system database connection is specified via a JDBC URL plus a username and password.
These values can be specified on the command line or via environment variables.

* System Database JDBC URL
  * `--db-url` / `-D` flag
  * `DBOS_SYSTEM_JDBC_URL` environment variable
* System Database user
  * `--db-user` / `-U` flag
  * `PGUSER` environment variable
* System Database password
  * `--db-password` / `-P` flag
  * `PGPASSWORD` environment variable

Example:
```bash
# Using command-line flag (highest priority)
dbos migrate --db-url jdbc:postgresql://localhost/mydb -U user -P password

# Using environment variable (lowest priority)
export DBOS_SYSTEM_JDBC_URL=jdbc:postgresql://localhost/mydb
export PGUSER=user
export PGPASSWORD=password
dbos migrate
```

### Database Schema Configuration

By default, DBOS creates its system tables in the `dbos` schema. You can specify a custom schema name using the `--schema` option.

Example:
```bash
# Use a custom schema for all DBOS system tables
dbos migrate --schema myapp_schema
```

## Commands

### `dbos migrate`
Create DBOS system tables in your database. This command runs the migration commands specified in the `database.migrate` section of your config file.

**Options:**
- `-r, --app-role <role>` - The role with which you will run your DBOS application
- `--[no-]listen-notify` - Use LISTEN/NOTIFY on the DBOS system database (default: enabled). Pass `--no-listen-notify` to disable.

**Usage:**
```bash
dbos migrate
dbos migrate --app-role myapp_role
dbos migrate --no-listen-notify
```

### `dbos reset`
Reset the DBOS system database, deleting metadata about past workflows and steps. _This is a permanent, destructive action!_

**Options:**
- `-y, --yes` - Skip confirmation prompt

**Usage:**
```bash
dbos reset
dbos reset --yes  # Skip confirmation
```

### `dbos version`
Show the version and exit.

**Usage:**
```bash
dbos --version
```

## License

See the main project [LICENSE](https://github.com/dbos-inc/dbos-transact-java/blob/main/LICENSE) file for details.