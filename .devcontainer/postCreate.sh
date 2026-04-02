#!/bin/bash

# Make the script executable and fail on any error
set -e

echo "🚀 Setting up DBOS Transact Java development environment..."

# Update package list
sudo apt-get update

# Install additional packages that might be useful
sudo apt-get install -y \
    postgresql-client \
    curl \
    wget \
    unzip \
    git \
    jq

# Ensure the Gradle wrapper is executable
chmod +x ./gradlew

# Download dependencies to warm up the cache
echo "📦 Downloading Gradle dependencies..."
./gradlew build --no-daemon --dry-run || true

# Apply code formatting (Spotless)
echo "🎨 Applying code formatting..."
./gradlew spotlessApply || true

# Set up Git safe directory (in case of permission issues)
git config --global --add safe.directory /workspaces/java-transact

# Make sure Docker is ready for testcontainers
echo "🐳 Checking Docker setup for testcontainers..."
docker --version

echo "✅ Development environment setup complete!"
echo ""
echo "🏗️  To build the project, run: ./gradlew build"
echo "🧪 To run tests, run: ./gradlew test"
echo "📖 To see available tasks, run: ./gradlew tasks"