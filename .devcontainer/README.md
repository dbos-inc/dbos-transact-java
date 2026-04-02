# DBOS Transact Java - Development Container

This directory contains the configuration for GitHub Codespaces and VS Code Dev Containers.

## What's Included

- **Java 21**: The required Java version for this project
- **Gradle**: Build system (using the wrapper in the project)
- **Docker-in-Docker**: For running testcontainers in tests

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

## Testing with PostgreSQL

The tests use **Testcontainers** which automatically spin up PostgreSQL instances as needed. No manual database setup required - just run `./gradlew test` and Testcontainers will handle the rest!

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

### Testcontainer Issues
If tests fail to start PostgreSQL containers:
```bash
# Check Docker is running
docker ps

# Check Docker permissions
docker run hello-world
```

## Environment Variables

You can set these environment variables for configuration:

- `GRADLE_OPTS`: Additional Gradle JVM options
- `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE`: Custom Docker socket path (if needed)

## Extensions Included

- **Java Extension Pack**: Core Java development tools
- **Kotlin**: Kotlin language support
- **Gradle for Java**: Gradle build system integration
- **Test Runner for Java**: JUnit test execution
- **GitHub Copilot**: AI-powered code completion (if you have access)
- **Docker**: Container management