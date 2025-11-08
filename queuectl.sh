#!/bin/bash

# Job Queue System - queuectl wrapper script
# This script provides an easy way to run the queuectl command

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Path to the JAR file
JAR_FILE="$SCRIPT_DIR/target/queuectl.jar"

# Check if JAR file exists
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: queuectl.jar not found at $JAR_FILE"
    echo "Please run 'mvn clean package' first to build the project."
    exit 1
fi

# Check if Java is available
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed or not in PATH"
    echo "Please install Java 17 or higher."
    exit 1
fi

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 17 ]; then
    echo "Error: Java 17 or higher is required. Found Java $JAVA_VERSION"
    exit 1
fi

# Run the application with all arguments passed through
exec java -jar "$JAR_FILE" "$@"
