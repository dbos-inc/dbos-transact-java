# DBOS Transact Java - Development Container

Simple devcontainer configuration for GitHub Codespaces and VS Code Dev Containers.

## What's Included

- **Java 21**: Pre-installed and ready to use
- **Docker-in-Docker**: For running Testcontainers in tests
- **VS Code Extensions**: Java Extension Pack

## Quick Start

### GitHub Codespaces
1. Click "Code" → "Codespaces" → "Create codespace"
2. Run `./gradlew build` to build the project
3. Run `./gradlew test` to run tests

### VS Code Dev Containers (Local)
1. Open project in VS Code with "Dev Containers" extension installed
2. Select "Reopen in Container" when prompted
3. Run `./gradlew build` to build the project

## Testing

Tests use **Testcontainers** to automatically spin up PostgreSQL instances - no manual setup needed!

```bash
./gradlew test
```