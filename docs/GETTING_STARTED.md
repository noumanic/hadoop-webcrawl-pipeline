# Getting Started

Quick start guide for the Hadoop Web Crawl Processing Pipeline.

## Quick Start

### Step 1: Extract Files (30 seconds)
```bash
# Extract/navigate to the project folder
cd hadoop-webcrawl-pipeline
# Project root should contain src/, scripts/, dataset-downloader/
ls
```

You should see:
- **src/** — Java source (7 files)
- **scripts/** — Build and run scripts
- **docs/** — Supplementary documentation
- **dataset-downloader/** — WET download notebook and paths

### Step 2: Make Scripts Executable (5 seconds)
```bash
chmod +x scripts/*.sh
```

### Step 3: Test Compilation (1 minute)
```bash
./scripts/compile.sh
```

Expected output:
```
==========================================
Compiling Hadoop MapReduce Pipeline
==========================================
Compiling Java source files...
Compilation successful!
Creating JAR file...
==========================================
Build complete!
JAR file: build/webcrawl-pipeline.jar
==========================================
```

### Step 4: Run Quick Test (2 minutes)
```bash
./scripts/test_sample.sh
```

This creates sample data and runs the full pipeline to verify everything works.

### Step 5: Process Your WET Files (45-90 minutes)
```bash
# Upload your WET files to HDFS
hadoop fs -mkdir -p /user/$(whoami)/input/wet_files
hadoop fs -put /path/to/your/wet/files/* /user/$(whoami)/input/wet_files/

# Run the pipeline (from project root)
./scripts/run_pipeline.sh /user/$(whoami)/input/wet_files /user/$(whoami)/output
```

## Contents

### Java Implementation (7 files)
1. **WebCrawlPipelineDriver.java** - Orchestrates all 6 jobs
2. **TextCleaningJob.java** - Stage 1: Clean raw text
3. **WordCountJob.java** - Stage 2: Count word frequencies
4. **WordLengthStatsJob.java** - Stage 3: Analyze word lengths
5. **AlphabetDistributionJob.java** - Stage 4: Letter distribution
6. **TopNWordsJob.java** - Stage 5: Extract top 1000 words
7. **FinalAnalysisJob.java** - Stage 6: Final analytics

### Automation Scripts (in scripts/)
1. **scripts/compile.sh** - Automated compilation and JAR creation
2. **scripts/run_pipeline.sh** - Execute full pipeline
3. **scripts/test_sample.sh** - Test with sample data

### Documentation (in docs/)
1. **docs/README.md** — Documentation index
2. **docs/ARCHITECTURE_DIAGRAM.md** — Pipeline architecture
3. **docs/DATA_DOWNLOAD_GUIDE.md** — WET download
4. **docs/QUICK_REFERENCE.md** — Command reference
5. **docs/PROJECT_REPORT.md** — Report template
6. **docs/FILE_MANIFEST.md** — File inventory

## 🎯 Pipeline Stages Explained

```
Stage 1: Text Cleaning
  ├─ Input:  Raw WET files (20GB)
  ├─ Action: Remove URLs, HTML, convert lowercase
  └─ Output: Clean words (8GB)

Stage 2: Word Count
  ├─ Input:  Clean words from Stage 1
  ├─ Action: Count frequency of each word
  └─ Output: Word counts (500MB)

Stage 3: Word Length Statistics
  ├─ Input:  Clean words from Stage 1
  ├─ Action: Analyze length distribution
  └─ Output: Length statistics (10KB)

Stage 4: Alphabet Distribution
  ├─ Input:  Clean words from Stage 1
  ├─ Action: Count letter frequencies
  └─ Output: Letter counts (5KB)

Stage 5: Top-N Words
  ├─ Input:  Word counts from Stage 2
  ├─ Action: Extract top 1000 words
  └─ Output: Top words list (50KB)

Stage 6: Final Analysis
  ├─ Input:  Stages 2 and 5 outputs
  ├─ Action: Filter, categorize, analyze
  └─ Output: Final results (200KB)
```

## Expected Results

After running the pipeline, you'll have:

### Stage 1 Output
```
hello
world
hadoop
mapreduce
distributed
...
```

### Stage 2 Output
```
the     15234
and     12456
data    8932
hadoop  7821
...
```

### Stage 6 Output
```
algorithm   count=1234, length=9, category=LONG, topN=true
compute     count=2345, length=7, category=MEDIUM, topN=true
...
### SUMMARY_STATISTICS ###   total_word_occurrences=1234567, unique_words=45678, avg_frequency=27.04
```

## Troubleshooting

### Problem: Compilation fails
**Solution**: Ensure HADOOP_HOME is set
```bash
export HADOOP_HOME=/path/to/hadoop
export HADOOP_CLASSPATH=$(hadoop classpath)
```

### Problem: "Input path does not exist"
**Solution**: Upload data to HDFS first
```bash
hadoop fs -mkdir -p /user/$(whoami)/input/wet_files
hadoop fs -put /path/to/files/* /user/$(whoami)/input/wet_files/
```

### Problem: "OutOfMemoryError"
**Solution**: Increase memory allocation
```bash
hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver \
    -D mapreduce.map.memory.mb=4096 \
    -D mapreduce.reduce.memory.mb=8192 \
    /input /output
```

### Problem: Job runs very slow
**Solution**: Adjust number of reducers
```bash
hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver \
    -D mapreduce.job.reduces=20 \
    /input /output
```

## Documentation

1. **Overview**: [../README.md](../README.md) (repository root)
2. **Commands**: [QUICK_REFERENCE.md](QUICK_REFERENCE.md)
3. **Architecture**: [ARCHITECTURE_DIAGRAM.md](ARCHITECTURE_DIAGRAM.md)
4. **Report**: [PROJECT_REPORT.md](PROJECT_REPORT.md)

## Pre-Submission Checklist

Before submitting your assignment:

- [ ] Compiled successfully without errors
- [ ] Tested with sample data (test_sample.sh passes)
- [ ] Processed your assigned WET files
- [ ] All 6 stages executed successfully
- [ ] Downloaded and reviewed results
- [ ] Filled out PROJECT_REPORT.md with your data
- [ ] Documented any customizations made
- [ ] Included screenshots/logs if required

## Learning Objectives

By completing this project, you will have:

✓ Implemented multi-stage MapReduce pipeline
✓ Processed large-scale real-world data (20GB)
✓ Used Hadoop HDFS and YARN
✓ Optimized with combiners and custom comparators
✓ Implemented data fusion with multiple inputs
✓ Generated meaningful analytics from raw text
✓ Demonstrated distributed computing principles

## Tips

1. **Test with small data first** - Use test_sample.sh before processing 20GB
2. **Monitor job progress** - Check ResourceManager UI at http://localhost:8088
3. **Check logs if fails** - Use `yarn logs -applicationId <app_id>`
4. **Start early** - Processing 20GB takes 45-90 minutes
5. **Document as you go** - Fill PROJECT_REPORT.md while running
6. **Save output samples** - Download results for your report

## Useful Commands (from project root)

```bash
# Compile
./scripts/compile.sh

# Test
./scripts/test_sample.sh

# Run full pipeline
./scripts/run_pipeline.sh /input /output

# View results
hadoop fs -cat /output/stage6_final/part-* | head -20

# Download results
hadoop fs -get /output ./results

# Clean up
hadoop fs -rm -r /output
```

---

**Quick Start Command Sequence (from project root):**
```bash
chmod +x scripts/*.sh            # Make executable
./scripts/compile.sh             # Compile
./scripts/test_sample.sh         # Test
# Upload your data to HDFS
./scripts/run_pipeline.sh /input /output  # Run
# Download and analyze results
```

**Estimated Time:**
- Setup: 5 minutes
- Testing: 2 minutes
- Full run: 45-90 minutes
- Analysis: 30 minutes
- **Total: ~2-3 hours**