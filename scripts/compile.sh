#!/usr/bin/env bash
# Hadoop MapReduce Pipeline Compilation Script
# Run from project root. WSL/Ubuntu compatible.

set -euo pipefail

# Run from project root (src/ must exist)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
if [ ! -d src ] || [ -z "$(echo src/*.java)" ]; then
    echo "ERROR: Run this script from the project root (directory containing src/ with .java files)."
    exit 1
fi

echo "=========================================="
echo "Compiling Hadoop MapReduce Pipeline"
echo "=========================================="

# Prefer the Hadoop install that is on PATH (so we compile against the same one that will run the JAR)
if command -v hadoop >/dev/null 2>&1; then
    _HADOOP_BIN=$(command -v hadoop)
    _HADOOP_DIR=$(cd "$(dirname "$_HADOOP_BIN")/.." && pwd)
    if [ -d "$_HADOOP_DIR/share/hadoop/common" ] || [ -d "$_HADOOP_DIR/share/hadoop/mapreduce" ] || [ -d "$_HADOOP_DIR/lib" ]; then
        export HADOOP_HOME="$_HADOOP_DIR"
        echo "Using HADOOP_HOME=$HADOOP_HOME (from 'hadoop' on PATH)"
    fi
fi

# Set Hadoop classpath
# 1) Try 'hadoop classpath' (filter out .sh entries that break javac)
# 2) If empty, build from HADOOP_HOME
RAW_CP=$(hadoop classpath 2>/dev/null) || true
if [ -n "$RAW_CP" ]; then
    HADOOP_CLASSPATH=$(echo "$RAW_CP" | tr '\n' ':' | sed 's/:[^:]*\.sh:/:/g; s/:[^:]*\.sh$//; s/^[^:]*\.sh://; s/^[^:]*\.sh$//; s/::*/:/g; s/^://; s/:$//')
else
    HADOOP_CLASSPATH=""
fi

if [ -z "$HADOOP_CLASSPATH" ] && [ -n "$HADOOP_HOME" ]; then
    echo "Building classpath from HADOOP_HOME=$HADOOP_HOME"
    CP=""
    # Hadoop 3.x layout (share/hadoop/...)
    for dir in "$HADOOP_HOME/share/hadoop/common" \
               "$HADOOP_HOME/share/hadoop/common/lib" \
               "$HADOOP_HOME/share/hadoop/mapreduce" \
               "$HADOOP_HOME/share/hadoop/mapreduce/lib" \
               "$HADOOP_HOME/share/hadoop/hdfs" \
               "$HADOOP_HOME/share/hadoop/hdfs/lib"; do
        if [ -d "$dir" ]; then
            for j in "$dir"/*.jar; do
                [ -f "$j" ] && CP="${CP}:${j}"
            done
        fi
    done
    # Hadoop 2.x layout (lib/ in root) if no JARs found yet
    if [ -z "$CP" ] && [ -d "$HADOOP_HOME/lib" ]; then
        for j in "$HADOOP_HOME/lib"/*.jar; do
            [ -f "$j" ] && CP="${CP}:${j}"
        done
    fi
    HADOOP_CLASSPATH="${CP#:}"
fi

if [ -z "$HADOOP_CLASSPATH" ]; then
    echo "ERROR: Could not build Hadoop classpath."
    echo "  1) Set HADOOP_HOME to your Hadoop install (e.g. export HADOOP_HOME=/usr/local/hadoop)"
    echo "  2) Or ensure 'hadoop classpath' prints JAR paths (run: hadoop classpath)"
    exit 1
fi
export HADOOP_CLASSPATH

# Create build directory
mkdir -p build/classes

# Compile all Java files (run from project root; sources in src/)
echo "Compiling Java source files..."
javac -classpath "$HADOOP_CLASSPATH" -d build/classes src/*.java

if [ $? -ne 0 ]; then
    echo "ERROR: Compilation failed!"
    exit 1
fi

echo "Compilation successful!"

# Create JAR file (from project root, build/classes contains compiled classes)
echo "Creating JAR file..."
cd build/classes
jar -cvf ../webcrawl-pipeline.jar ./*.class

if [ $? -ne 0 ]; then
    echo "ERROR: JAR creation failed!"
    exit 1
fi

cd "$PROJECT_ROOT"

echo "=========================================="
echo "Build complete!"
echo "JAR file: build/webcrawl-pipeline.jar"
echo "=========================================="

# Show JAR contents
echo ""
echo "JAR contents:"
jar -tf build/webcrawl-pipeline.jar

echo ""
echo "To run the pipeline:"
echo "hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver <input> <output>"