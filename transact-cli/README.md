# DBOS CLI

The DBOS CLI is a command-line interface for managing DBOS workflows.

## Installation

DBOS CLI is distributed as a JAR with dependencies (also known as a fat or uber JAR).
It is run from the command line with the `-jar` option of the [`java` command](https://docs.oracle.com/en/java/javase/17/docs/specs/man/java.html).

```shell
$ java -jar dbos.jar --version
dbos v0.7.0
```

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
* JDBC URL
  * `--db-password` / `-P` flag
  * `PGPASSWORD` environment variable

Example:
```bash
# Using command-line flag (highest priority)
java -jar dbos.jar migrate --db-url postgres://localhost/mydb -U user -P password

# Using environment variable (lowest priority)
export DBOS_SYSTEM_JDBC_URL=postgres://localhost/mydb
export PGUSER=user
export PGPASSWORD=password
java -jar dbos.jar migrate
```

### `dbos migrate`
Create DBOS system tables in your database. This command runs the migration commands specified in the `database.migrate` section of your config file.

**Options:**
- `-r, --app-role <role>` - The role with which you will run your DBOS application

**Usage:**
```bash
java -jar dbos.jar migrate
java -jar dbos.jar migrate --app-role myapp_role
```

### `dbos reset`
Reset the DBOS system database, deleting metadata about past workflows and steps. _This is a permanent, destructive action!_

**Options:**
- `-y, --yes` - Skip confirmation prompt

**Usage:**
```bash
java -jar dbos.jar reset
java -jar dbos.jar reset --yes  # Skip confirmation
```

### `dbos postgres`
Manage a local PostgreSQL database with Docker for development.

#### `dbos postgres start`
Start a local Postgres database container with pgvector extension.

**Options:**
- `-c, --container-name` - Docker container name, defaults to dbos-db
- `-i, --image-name` - Docker image name, defaults to pgvector/pgvector:pg16

**Usage:**
```bash
java -jar dbos.jar postgres start
```

Creates a PostgreSQL container with:
- Container name: `dbos-db` (unless overridden by `--container-name`)
- Port: 5432
- Default database: `dbos`
- Default user: `postgres`
- Default password: `dbos`

#### `dbos postgres stop`
Stop the local Postgres database container.

**Usage:**
```bash
java -jar dbos.jar postgres stop
```
**Options:**
- `-c, --container-name` - Docker container name, defaults to dbos-db

### `dbos workflow`
Manage DBOS workflows.

#### `dbos workflow list`
List workflows for your application.

**Options:**
- `-l, --limit <number>` - Limit the results returned (default: 10)
- `-o, --offset <number>` - Offset for pagination
- `-S, --status <status>` - Filter by status (PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)
- `-n, --name <name>` - Retrieve workflows with this name
- `-v, --application-version <version>` - Retrieve workflows with this application version
- `-s, --start-time <timestamp>` - Retrieve workflows starting after this timestamp (ISO 8601)
- `-e, --end-time <timestamp>` - Retrieve workflows starting before this timestamp (ISO 8601)
- `-q, --queue <queue>` - Retrieve workflows on this queue
- `-Q, --queues-only` - Retrieve only queued workflows
- `-d, --sort-desc` - Sort the results in descending order (older first)

**Usage:**
```bash
java -jar dbos.jar workflow list
java -jar dbos.jar workflow list --limit 50 --status SUCCESS
java -jar dbos.jar workflow list --name "ProcessOrder" --user "admin"
```

#### `dbos workflow get [workflow-id]`
Retrieve the status of a specific workflow.

**Usage:**
```bash
java -jar dbos.jar workflow get abc123def456
```

Returns detailed information about the workflow including its status, start time, end time, and other metadata.

#### `dbos workflow steps [workflow-id]`
List the steps of a workflow.

**Usage:**
```bash
java -jar dbos.jar workflow steps abc123def456
```

Shows all the steps executed within a workflow, their status, and execution details.

#### `dbos workflow cancel [workflow-id]`
Cancel a workflow so it is no longer automatically retried or restarted.

**Usage:**
```bash
java -jar dbos.jar workflow cancel abc123def456
```

#### `dbos workflow resume [workflow-id]`
Resume a workflow that has been cancelled.

**Usage:**
```bash
java -jar dbos.jar workflow resume abc123def456
```

#### `dbos workflow fork [workflow-id]`
Fork a workflow from the beginning or from a specific step.

**Options:**
- `-s, --step <number>` - Restart from this step (default: 1)
- `-f, --forked-workflow-id <id>` - Custom workflow ID for the forked workflow
- `-a, --application-version <version>` - Application version for the forked workflow

**Usage:**
```bash
java -jar dbos.jar workflow fork abc123def456
java -jar dbos.jar workflow fork abc123def456 --step 3
java -jar dbos.jar workflow fork abc123def456 --forked-workflow-id custom-id-123
```

### `dbos version`
Show the version and exit.

**Usage:**
```bash
java -jar dbos.jar --version
```

## License

See the main project [LICENSE](https://github.com/dbos-inc/dbos-transact-java/blob/main/LICENSE) file for details.