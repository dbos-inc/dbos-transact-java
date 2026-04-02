# DBOS Transact Java - Development Container

This directory contains the configuration for GitHub Codespaces and VS Code Dev Containers.

## What's Included

- **Java 17**: The required Java version for this project
- **Gradle**: Build system (using the wrapper in the project)
- **Docker-in-Docker**: For running testcontainers in tests
- **PostgreSQL Client**: For connecting to databases
- **VS Code Extensions**: Java development pack, Kotlin support, Gradle integration

## Quick Start

### Using GitHub Codespaces

1. Go to the repository on GitHub
2. Click the green "Code" button
3. Select "Codespaces" tab
4. Click "Create codespace on [branch-name]"
5. Wait for the environment to set up (this may take a few minutes)
6. Run `./gradlew build` to build the project

### Using VS Code Dev Containers (Local)

1. Install Docker Desktop and VS Code with the "Dev Containers" extension
2. Open the project in VS Code
3. When prompted, click "Reopen in Container" (or use Command Palette: "Dev Containers: Reopen in Container")
4. Wait for the container to build and set up
5. Run `./gradlew build` to build the project

## Available Services

### Local PostgreSQL (Optional)

A PostgreSQL database is available via docker-compose for local development:

```bash
# Start PostgreSQL
docker-compose -f .devcontainer/docker-compose.yml up -d postgres

# Connect to the database
psql -h localhost -U postgres -d dbos_test
# Password: dbos

# Stop PostgreSQL
docker-compose -f .devcontainer/docker-compose.yml down
```

**Note**: The tests use Testcontainers which automatically spin up PostgreSQL instances, so the local PostgreSQL 16 is optional and mainly for manual testing or debugging.

## Common Commands

```bash
# Build the project
./gradlew build

# Run tests
./gradlew test

# Run tests with verbose output
./gradlew test --info

# Clean build artifacts
./gradlew clean

# See all available tasks
./gradlew tasks

# Check for dependency updates
./gradlew dependencyUpdates
```

## Troubleshooting

### Docker Issues
If you encounter Docker permission issues:
```bash
sudo usermod -aG docker vscode
# Then restart the container
```

### Gradle Wrapper Issues
If the Gradle wrapper isn't executable:
```bash
chmod +x ./gradlew
```

### Port Conflicts
If port 5432 is already in use:
```bash
# Check what's using the port
sudo lsof -i :5432

# Stop the conflicting service or modify docker-compose.yml to use a different port
```

## Environment Variables

You can set these environment variables for configuration:

- `DBOS_POSTGRES_URL`: Custom PostgreSQL connection string
- `GRADLE_OPTS`: Additional Gradle JVM options

## Extensions Included

- **Java Extension Pack**: Core Java development tools
- **Kotlin**: Kotlin language support
- **Gradle for Java**: Gradle build system integration
- **Test Runner for Java**: JUnit test execution
- **GitHub Copilot**: AI-powered code completion (if you have access)
- **Docker**: Container management