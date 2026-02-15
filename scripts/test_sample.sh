#!/usr/bin/env bash
# Test Script for Hadoop MapReduce Pipeline
# Run from project root. WSL/Ubuntu compatible.

set -euo pipefail

# Run from project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Pipeline Test Script"
echo "=========================================="

# Create test data directory
mkdir -p test_data

# Generate sample WET-like content
cat > test_data/sample1.txt << 'EOF'
WARC/1.0
WARC-Type: conversion
Content-Type: text/plain

The Apache Hadoop software library is a framework that allows for the distributed 
processing of large data sets across clusters of computers using simple programming models.
It is designed to scale up from single servers to thousands of machines, each offering 
local computation and storage.

Hadoop MapReduce is a software framework for easily writing applications which process 
vast amounts of data in-parallel on large clusters of commodity hardware in a reliable, 
fault-tolerant manner. MapReduce has become a popular data processing model for large 
scale applications.

The data processing pipeline begins with raw text extraction from web crawl files.
These files contain cleaned textual content that has been extracted from webpages.
Natural language processing techniques can then be applied to analyze the text.
EOF

cat > test_data/sample2.txt << 'EOF'
Web crawling is the process of systematically browsing the internet to collect data.
Search engines use web crawlers to index content across millions of websites.
The crawler starts with a list of URLs and visits each page to extract text and links.

Modern search systems rely on distributed computing frameworks like Apache Hadoop.
Big data analytics requires processing terabytes of information efficiently.
MapReduce enables parallel processing across multiple compute nodes simultaneously.
The reduce phase aggregates results from all mappers into final output.

Text mining and natural language processing extract insights from unstructured data.
Word frequency analysis reveals the most common terms in a document corpus.
Statistical methods help identify patterns and trends in large text collections.
EOF

cat > test_data/sample3.txt << 'EOF'
Distributed systems enable processing massive datasets that cannot fit on a single machine.
Hadoop provides both storage (HDFS) and computation (MapReduce) capabilities.
The framework handles task scheduling, failure recovery, and data distribution automatically.

Data scientists use these tools to analyze user behavior, market trends, and social media.
Machine learning algorithms require large training datasets to achieve high accuracy.
Feature extraction from text involves tokenization, stemming, and removing stop words.

The final stage of the pipeline produces analytical insights and summary statistics.
Top-N queries identify the most frequent items in a dataset efficiently.
Scalability is achieved through horizontal partitioning and parallel execution.
EOF

echo "Sample test data created in test_data/"
echo ""

# Check if JAR exists; compile if not
if [ ! -f build/webcrawl-pipeline.jar ]; then
    echo "JAR file not found. Running compilation..."
    "$SCRIPT_DIR/compile.sh"
    if [ $? -ne 0 ]; then
        echo "Compilation failed!"
        exit 1
    fi
fi

HDFS_USER="/user/$(whoami)"
# Setup HDFS test directories (ignore rm errors if dirs do not exist)
echo "Setting up HDFS test directories..."
hadoop fs -rm -r "$HDFS_USER/test_input" 2>/dev/null || true
hadoop fs -rm -r "$HDFS_USER/test_output" 2>/dev/null || true
hadoop fs -mkdir -p "$HDFS_USER/test_input"

# Upload test data
echo "Uploading test data to HDFS..."
hadoop fs -put test_data/*.txt "$HDFS_USER/test_input/"

# Verify upload
echo ""
echo "Test files in HDFS:"
hadoop fs -ls "$HDFS_USER/test_input/"
echo ""

# Run pipeline
echo "Running pipeline on test data..."
echo "This should complete in 1-2 minutes..."
echo ""

hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver \
    "$HDFS_USER/test_input" \
    "$HDFS_USER/test_output"

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "Test Completed Successfully!"
    echo "=========================================="
    echo ""
    
    echo "=== Stage 1: Cleaned Text (first 10 words) ==="
    hadoop fs -cat "$HDFS_USER/test_output/stage1_cleaned/part-*" 2>/dev/null | head -10
    echo ""
    
    echo "=== Stage 2: Top 15 Word Counts ==="
    hadoop fs -cat "$HDFS_USER/test_output/stage2_wordcount/part-*" | \
        sort -t$'\t' -k2 -rn | head -15
    echo ""
    
    echo "=== Stage 3: Word Length Distribution ==="
    hadoop fs -cat "$HDFS_USER/test_output/stage3_wordlength/part-*"
    echo ""
    
    echo "=== Stage 4: First Letter Distribution (a-e) ==="
    hadoop fs -cat "$HDFS_USER/test_output/stage4_alphabet/part-*" | \
        grep "^FIRST_[a-e]" | head -5
    echo ""
    
    echo "=== Stage 5: Top 20 Words ==="
    hadoop fs -cat "$HDFS_USER/test_output/stage5_topn/part-*" | head -20
    echo ""
    
    echo "=== Stage 6: Summary Statistics ==="
    hadoop fs -cat "$HDFS_USER/test_output/stage6_final/part-*" | \
        grep "SUMMARY_STATISTICS"
    echo ""
    
    echo "=== Stage 6: Sample Analysis (first 10 entries) ==="
    hadoop fs -cat "$HDFS_USER/test_output/stage6_final/part-*" | \
        grep -v "SUMMARY" | head -10
    echo ""
    
    echo "=========================================="
    echo "Download full results:"
    echo "  hadoop fs -get $HDFS_USER/test_output ./test_results"
    echo ""
    echo "Clean up test data:"
    echo "  hadoop fs -rm -r $HDFS_USER/test_input"
    echo "  hadoop fs -rm -r $HDFS_USER/test_output"
    echo "  rm -rf test_data"
    echo "=========================================="
else
    echo ""
    echo "=========================================="
    echo "Test Failed!"
    echo "=========================================="
    echo "Check error messages above"
    exit 1
fi