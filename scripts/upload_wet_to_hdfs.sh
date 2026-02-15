#!/usr/bin/env bash
# Upload all WET files from dataset-downloader to HDFS for pipeline processing.
# Run from project root. WSL/Ubuntu compatible.

set -euo pipefail

# Ensure we are in project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

WET_DIR="dataset-downloader/downloaded_wet_files"
HDFS_INPUT="${1:-/user/$(whoami)/input/wet_files}"

if [ ! -d "$WET_DIR" ]; then
    echo "ERROR: WET directory not found: $WET_DIR"
    echo "Run from project root. Download files first using dataset-downloader/download_common_crawl.ipynb"
    exit 1
fi

# Count data-* files (no match yields 0 on Ubuntu)
FILE_COUNT=$(find "$WET_DIR" -maxdepth 1 -name 'data-*' -type f 2>/dev/null | wc -l | tr -d ' \n\r')
if [ -z "$FILE_COUNT" ] || [ "$FILE_COUNT" -eq 0 ]; then
    echo "ERROR: No data-* files in $WET_DIR"
    echo "Download WET files using the Jupyter notebook in dataset-downloader/"
    exit 1
fi

echo "=========================================="
echo "Upload WET files to HDFS"
echo "=========================================="
echo "Local directory: $WET_DIR"
echo "HDFS path:      $HDFS_INPUT"
echo "File count:     $FILE_COUNT"
echo ""

if ! hadoop fs -mkdir -p "$HDFS_INPUT" 2>/dev/null; then
    echo "ERROR: Cannot reach HDFS (Connection refused or similar)."
    echo "Start HDFS and YARN first, for example:"
    echo "  \$HADOOP_HOME/sbin/start-dfs.sh"
    echo "  \$HADOOP_HOME/sbin/start-yarn.sh"
    echo "Then run this script again."
    exit 1
fi
echo "Uploading $FILE_COUNT files (this may take several minutes)..."
# Shell glob: data-* (we already verified FILE_COUNT > 0)
hadoop fs -put "$WET_DIR"/data-* "$HDFS_INPUT/"

UPLOADED=$(hadoop fs -ls "$HDFS_INPUT" 2>/dev/null | grep -v 'Found' | wc -l | tr -d ' \n\r')
echo ""
echo "=========================================="
echo "Upload complete. $UPLOADED file(s) in $HDFS_INPUT"
echo "=========================================="
echo "Run the pipeline:"
echo "  ./scripts/run_pipeline.sh $HDFS_INPUT /user/$(whoami)/output"
echo ""
