#!/usr/bin/env bash
# Hadoop MapReduce Pipeline Execution Script
# Run from project root. WSL/Ubuntu compatible.

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 <input_directory> <output_base_directory>"
    echo "Example: $0 /user/$(whoami)/wet_files /user/$(whoami)/output"
    exit 1
fi

INPUT_DIR="$1"
OUTPUT_DIR="$2"

# Ensure we are in project root (must have src/ so paths to JAR and scripts resolve)
if [ ! -d src ] || [ ! -f "src/WebCrawlPipelineDriver.java" ]; then
    echo "ERROR: Run this script from the project root (directory containing src/)."
    exit 1
fi

echo "=========================================="
echo "Hadoop MapReduce Pipeline Execution"
echo "=========================================="
echo "Input Directory: $INPUT_DIR"
echo "Output Directory: $OUTPUT_DIR"
echo ""

# Check if input exists
if ! hadoop fs -test -e "$INPUT_DIR" 2>/dev/null; then
    echo "ERROR: Input directory does not exist in HDFS!"
    echo "Please upload your WET files first (all 99 files):"
    echo "  hadoop fs -mkdir -p $INPUT_DIR"
    echo "  hadoop fs -put dataset-downloader/downloaded_wet_files/data-* $INPUT_DIR/"
    echo "  (or: hadoop fs -put dataset-downloader/downloaded_wet_files/* $INPUT_DIR/)"
    exit 1
fi

# Report how many input files will be processed (all files in directory)
INPUT_COUNT=$(hadoop fs -ls "$INPUT_DIR" 2>/dev/null | grep -v 'Found' | wc -l | tr -d ' \n\r')
if [ -z "$INPUT_COUNT" ] || [ "$INPUT_COUNT" -eq 0 ]; then
    echo "WARNING: No files found in input directory. Check path and upload."
    echo "  To upload all 99 WET files from project root:"
    echo "  hadoop fs -put dataset-downloader/downloaded_wet_files/data-* $INPUT_DIR/"
    exit 1
fi
echo "Input directory contains $INPUT_COUNT file(s). All will be processed."
echo ""

# Remove existing output directory (ignore error if it does not exist)
echo "Cleaning previous output directory..."
hadoop fs -rm -r "$OUTPUT_DIR" 2>/dev/null || true

# Check if JAR exists
if [ ! -f build/webcrawl-pipeline.jar ]; then
    echo "ERROR: JAR file not found!"
    echo "Please run ./scripts/compile.sh first (from project root)"
    exit 1
fi

# Run the pipeline
echo ""
echo "Starting pipeline execution..."
echo "This may take several minutes depending on data size..."
echo ""

hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver "$INPUT_DIR" "$OUTPUT_DIR"

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "Pipeline completed successfully!"
    echo "=========================================="
    echo ""
    echo "View results:"
    echo "  Stage 1 (Cleaned Text):     hadoop fs -cat $OUTPUT_DIR/stage1_cleaned/part-* | head -20"
    echo "  Stage 2 (Word Counts):      hadoop fs -cat $OUTPUT_DIR/stage2_wordcount/part-* | head -20"
    echo "  Stage 3 (Word Length):      hadoop fs -cat $OUTPUT_DIR/stage3_wordlength/part-*"
    echo "  Stage 4 (Alphabet Dist):    hadoop fs -cat $OUTPUT_DIR/stage4_alphabet/part-*"
    echo "  Stage 5 (Top-N Words):      hadoop fs -cat $OUTPUT_DIR/stage5_topn/part-*"
    echo "  Stage 6 (Final Analysis):   hadoop fs -cat $OUTPUT_DIR/stage6_final/part-*"
    echo ""
    echo "Download all results:"
    echo "  hadoop fs -get $OUTPUT_DIR ./results"
else
    echo ""
    echo "=========================================="
    echo "Pipeline execution failed!"
    echo "=========================================="
    echo "Check the logs above for error details"
    exit 1
fi