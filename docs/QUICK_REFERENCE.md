# Quick Reference

Command reference for the Hadoop Web Crawl Processing Pipeline.

## Quick Start (3 Steps)

Run from **project root** (hadoop-webcrawl-pipeline/).

```bash
# 1. Compile
./scripts/compile.sh

# 2. Upload data to HDFS
hadoop fs -mkdir -p /user/$(whoami)/input/wet_files
hadoop fs -put /path/to/wet/files/* /user/$(whoami)/input/wet_files/

# 3. Run pipeline
./scripts/run_pipeline.sh /user/$(whoami)/input/wet_files /user/$(whoami)/output
```

## Essential Commands

### Compilation (from project root)
```bash
# Full compilation
./scripts/compile.sh

# Manual compilation (if script fails)
export HADOOP_CLASSPATH=$(hadoop classpath)
javac -classpath $HADOOP_CLASSPATH -d build/classes src/*.java
cd build/classes && jar -cvf ../webcrawl-pipeline.jar *.class && cd ../..
```

### Data Preparation
```bash
# Create HDFS directories
hadoop fs -mkdir -p /user/$(whoami)/input/wet_files

# Upload files
hadoop fs -put local_file.txt /user/$(whoami)/input/wet_files/
hadoop fs -put /path/to/directory/* /user/$(whoami)/input/wet_files/

# Verify upload
hadoop fs -ls /user/$(whoami)/input/wet_files/
hadoop fs -du -h /user/$(whoami)/input/wet_files/
```

### Execution (from project root)
```bash
# Run full pipeline
./scripts/run_pipeline.sh <input> <output>

# Run with custom parameters
hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver \
    -D mapreduce.job.reduces=15 \
    -D mapreduce.map.memory.mb=3072 \
    -D mapreduce.reduce.memory.mb=6144 \
    /user/$(whoami)/input/wet_files \
    /user/$(whoami)/output
```

### Testing (from project root)
```bash
# Quick test with sample data
./scripts/test_sample.sh

# Manual test
echo "test data test" > test.txt
hadoop fs -put test.txt /user/$(whoami)/test/
hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver \
    /user/$(whoami)/test /user/$(whoami)/test_out
```

### Viewing Results
```bash
# View specific stage output
hadoop fs -cat /user/$(whoami)/output/stage1_cleaned/part-* | head -20
hadoop fs -cat /user/$(whoami)/output/stage2_wordcount/part-* | sort -rn -k2 | head -20
hadoop fs -cat /user/$(whoami)/output/stage3_wordlength/part-*
hadoop fs -cat /user/$(whoami)/output/stage4_alphabet/part-*
hadoop fs -cat /user/$(whoami)/output/stage5_topn/part-*
hadoop fs -cat /user/$(whoami)/output/stage6_final/part-*

# Download all results
hadoop fs -get /user/$(whoami)/output ./results
```

### Monitoring
```bash
# List running jobs
mapred job -list

# Check job status
mapred job -status <job_id>

# View job logs
yarn logs -applicationId <application_id>

# ResourceManager UI (web interface)
# Open browser to: http://localhost:8088
```

### Troubleshooting
```bash
# Check HDFS health
hadoop fsck /

# View HDFS usage
hadoop fs -df -h

# Check if file exists
hadoop fs -test -e /path/to/file && echo "exists" || echo "not found"

# Remove output directory (if job fails and you need to rerun)
hadoop fs -rm -r /user/$(whoami)/output

# View detailed job counters
mapred job -counter <job_id> <group> <counter>
```

### Cleanup
```bash
# Remove HDFS data
hadoop fs -rm -r /user/$(whoami)/input
hadoop fs -rm -r /user/$(whoami)/output
hadoop fs -rm -r /user/$(whoami)/test*

# Remove local files
rm -rf build/ test_data/ results/
```

## Performance Tuning Parameters

### Small Dataset (< 5GB)
```bash
-D mapreduce.job.reduces=5
-D mapreduce.map.memory.mb=1024
-D mapreduce.reduce.memory.mb=2048
```

### Medium Dataset (5-15GB)
```bash
-D mapreduce.job.reduces=10
-D mapreduce.map.memory.mb=2048
-D mapreduce.reduce.memory.mb=4096
```

### Large Dataset (> 15GB)
```bash
-D mapreduce.job.reduces=20
-D mapreduce.map.memory.mb=3072
-D mapreduce.reduce.memory.mb=6144
-D mapreduce.task.io.sort.mb=512
```

### Enable Compression
```bash
-D mapreduce.map.output.compress=true
-D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
-D mapreduce.output.fileoutputformat.compress=true
-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec
```

## Common Issues and Solutions

### Issue: "OutOfMemoryError"
```bash
# Solution: Increase memory allocation
hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver \
    -D mapreduce.map.memory.mb=4096 \
    -D mapreduce.reduce.memory.mb=8192 \
    -D mapreduce.map.java.opts=-Xmx3276m \
    -D mapreduce.reduce.java.opts=-Xmx6553m \
    /input /output
```

### Issue: "Input path does not exist"
```bash
# Solution: Verify HDFS path
hadoop fs -ls /user/$(whoami)/input/wet_files
# If missing, upload data:
hadoop fs -mkdir -p /user/$(whoami)/input/wet_files
hadoop fs -put /path/to/files/* /user/$(whoami)/input/wet_files/
```

### Issue: "Output directory already exists"
```bash
# Solution: Remove existing output
hadoop fs -rm -r /user/$(whoami)/output
# Or use different output path
```

### Issue: "Permission denied"
```bash
# Solution: Fix HDFS permissions
hadoop fs -chmod -R 755 /user/$(whoami)
```

### Issue: Job hangs or runs very slowly
```bash
# Check cluster resources
yarn node -list
yarn application -list

# View running tasks
mapred job -list

# Kill hanging job
mapred job -kill <job_id>
```

## Job-Specific Commands

### Run Individual Jobs (for debugging)
```bash
# Job 1 only
hadoop jar build/webcrawl-pipeline.jar TextCleaningJob \
    /input /output/stage1

# Job 2 only (requires Job 1 output)
hadoop jar build/webcrawl-pipeline.jar WordCountJob \
    /output/stage1 /output/stage2

# And so on...
```

## Data Analysis Commands

### Top 100 Words
```bash
hadoop fs -cat /user/$(whoami)/output/stage2_wordcount/part-* | \
    sort -t$'\t' -k2 -rn | head -100
```

### Words by Length
```bash
hadoop fs -cat /user/$(whoami)/output/stage3_wordlength/part-* | \
    sort -t$'\t' -k1 -n
```

### Starting Letter Distribution
```bash
hadoop fs -cat /user/$(whoami)/output/stage4_alphabet/part-* | \
    grep "^FIRST_" | sort -t$'\t' -k2 -rn
```

### Export to CSV
```bash
# Word counts to CSV
hadoop fs -cat /user/$(whoami)/output/stage2_wordcount/part-* | \
    awk '{print $1","$2}' > wordcounts.csv

# Top-N words to CSV
hadoop fs -cat /user/$(whoami)/output/stage5_topn/part-* | \
    awk '{print NR","$1","$2}' > topwords.csv
```

## Environment Variables

```bash
# Set Hadoop home
export HADOOP_HOME=/path/to/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Set Java home
export JAVA_HOME=/path/to/jdk
export PATH=$PATH:$JAVA_HOME/bin

# Verify
echo $HADOOP_HOME
echo $JAVA_HOME
hadoop version
java -version
```

## Useful Aliases

```bash
# Add to ~/.bashrc or ~/.bash_profile

alias hls='hadoop fs -ls'
alias hcat='hadoop fs -cat'
alias hrm='hadoop fs -rm -r'
alias hmkdir='hadoop fs -mkdir -p'
alias hput='hadoop fs -put'
alias hget='hadoop fs -get'
alias hdu='hadoop fs -du -h'
alias pipeline='cd ~/hadoop-webcrawl-pipeline && ./scripts/run_pipeline.sh'
```

## Performance Benchmarking

```bash
# Time the execution (from project root)
time ./scripts/run_pipeline.sh /input /output

# Get detailed statistics
hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver \
    /input /output 2>&1 | tee pipeline.log

# Extract timing from logs
grep "completed in" pipeline.log
```

## File Size Estimation

```bash
# Check input size
hadoop fs -du -s -h /user/$(whoami)/input/wet_files

# Check output sizes
for dir in stage{1..6}*; do
    echo -n "$dir: "
    hadoop fs -du -s -h /user/$(whoami)/output/$dir
done

# Total output size
hadoop fs -du -s -h /user/$(whoami)/output
```

---

**Tip**: Bookmark this file for quick access to essential commands!