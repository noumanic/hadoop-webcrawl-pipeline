# Hadoop Web Crawl Processing Pipeline

![Hadoop](https://img.shields.io/badge/Hadoop-3.x-66CCFF?style=flat&logo=apache-hadoop)
![Java](https://img.shields.io/badge/Java-8+-007396?style=flat&logo=java)
![MapReduce](https://img.shields.io/badge/MapReduce-Pipeline-orange?style=flat)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

A six-stage Hadoop MapReduce pipeline for distributed text analytics over large-scale web crawl (WET) data.

## Overview

This repository implements a sequential six-job MapReduce pipeline that processes Web Extracted Text (WET) files from Common Crawl. The pipeline performs text cleaning and normalization, word-count aggregation, word-length and alphabet-distribution statistics, top-N frequent word extraction, and a final filtered analytical summary. All jobs are orchestrated by a single driver; each job uses distinct mappers and reducers.

## Architecture

The pipeline is orchestrated by **WebCrawlPipelineDriver**, which runs six MapReduce jobs in sequence. Stage 1 writes cleaned text to HDFS; stages 2, 3, and 4 read from stage 1 in parallel; stage 5 reads from stage 2; stage 6 reads from both stage 2 and stage 5.

**High-level flow:**

```
WET Files (~20 GB) → Stage 1 (Text Cleaning) → stage1_cleaned
                         ├→ Stage 2 (Word Count)     → stage2_wordcount ─┬→ Stage 5 (Top-N) → stage5_topn ─┐
                         ├→ Stage 3 (Word Length)    → stage3_wordlength  │                                    ├→ Stage 6 (Final Analysis) → stage6_final
                         └→ Stage 4 (Alphabet)      → stage4_alphabet    └──────────────────────────────────┘
```

**Detailed diagram:** A full Mermaid diagram (data flow, mapper/reducer names, HDFS paths, and legend) is in [docs/pipeline-architecture.mmd](docs/pipeline-architecture.mmd). Render it with [Mermaid Live](https://mermaid.live), VS Code (Mermaid extension), or any Mermaid-capable viewer.

**Execution expectations:** Four separate Mermaid diagrams for (1) successful WET file processing, (2) execution of all six jobs, (3) distributed processing behaviour, and (4) Driver orchestration are in [docs/execution-expectations.md](docs/execution-expectations.md).

### Job Descriptions

#### **Job 1: Text Cleaning and Normalization**
- **Purpose**: Clean raw WET file content
- **Operations**:
  - Remove URLs and email addresses
  - Strip HTML entities
  - Convert to lowercase
  - Remove non-alphabetic characters
  - Filter words shorter than 2 characters
- **Input**: Raw WET files
- **Output**: Cleaned words (one per line)

#### **Job 2: Word Count Aggregation**
- **Purpose**: Count frequency of each word
- **Operations**:
  - Classic word count with combiner optimization
  - Distributed aggregation across reducers
- **Input**: Cleaned text from Job 1
- **Output**: (word, count) pairs
- **Optimization**: Uses combiner for local aggregation

#### **Job 3: Word Length Statistics**
- **Purpose**: Analyze word length distribution
- **Operations**:
  - Group words by length
  - Calculate count and average per partition
- **Input**: Cleaned text from Job 1
- **Output**: (length, statistics) pairs

#### **Job 4: Alphabet Distribution Analysis**
- **Purpose**: Analyze letter frequency patterns
- **Operations**:
  - Count first letter of each word
  - Count total character occurrences
  - Compute distribution statistics
- **Input**: Cleaned text from Job 1
- **Output**: Letter frequency data

#### **Job 5: Top-N Frequent Words Identification**
- **Purpose**: Identify most common words
- **Operations**:
  - Secondary sort by frequency (descending)
  - Extract top 1000 words
- **Input**: Word counts from Job 2
- **Output**: Top 1000 words with counts
- **Optimization**: Single reducer for global ranking

#### **Job 6: Final Filtered Ranking and Analytical Summary**
- **Purpose**: Generate comprehensive analytics
- **Operations**:
  - Filter stop words
  - Combine data from Jobs 2 and 5
  - Categorize words by length
  - Generate summary statistics
- **Input**: Multiple inputs from Jobs 2 and 5
- **Output**: Filtered rankings with analytics

## Project Structure

All commands are intended to be run from the **project root**.

```
hadoop-webcrawl-pipeline/
├── README.md                 # This file
├── LICENSE                   # MIT License
├── .gitignore
├── src/                      # Java source (7 files)
│   ├── WebCrawlPipelineDriver.java
│   ├── TextCleaningJob.java
│   ├── WordCountJob.java
│   ├── WordLengthStatsJob.java
│   ├── AlphabetDistributionJob.java
│   ├── TopNWordsJob.java
│   └── FinalAnalysisJob.java
├── scripts/
│   ├── compile.sh            # Build JAR
│   ├── run_pipeline.sh       # Run full pipeline
│   ├── upload_wet_to_hdfs.sh # Upload WET files to HDFS
│   └── test_sample.sh        # Sample run
├── docs/                     # Supplementary documentation
│   ├── README.md             # Documentation index
│   ├── pipeline-architecture.mmd   # Mermaid pipeline diagram (detailed)
│   ├── execution-expectations.md   # Four Mermaid diagrams (WET, 6 jobs, distributed, Driver)
│   ├── ARCHITECTURE_DIAGRAM.md     # Text architecture notes
│   ├── DATA_DOWNLOAD_GUIDE.md
│   ├── GETTING_STARTED.md
│   ├── FILE_MANIFEST.md
│   ├── QUICK_REFERENCE.md
│   └── PROJECT_REPORT.md
└── dataset-downloader/
    ├── download_common_crawl.ipynb
    └── wet.paths
```

Further documentation (architecture, data download, quick reference, report template) is in [docs/](docs/README.md).

## Prerequisites

- **Hadoop**: 2.x or 3.x, with HDFS (and YARN if running in cluster mode)
- **Java**: JDK 8 or higher
- **Data**: WET files in a single HDFS directory (e.g. ~99 files, ~20GB total); the pipeline processes every file in the given input path

## Setup

### 1. Repository layout

Ensure the project root contains `src/`, `scripts/`, and `docs/`. Source files are under `src/`.

### 2. Start Hadoop (HDFS and YARN)

HDFS and YARN must be running before upload or pipeline execution.

**Option A: Start daemons directly (no SSH, recommended for WSL/single-node)**  
Avoids SSH to localhost. Use if `start-dfs.sh` fails with "ssh: connect to host localhost port 22: Connection refused":

```bash
# Hadoop 3.x (hdfs / yarn on PATH)
hdfs --daemon start namenode
hdfs --daemon start datanode
hdfs --daemon start secondarynamenode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
```

To stop later: replace `start` with `stop` in the same commands (e.g. `hdfs --daemon stop namenode`).

**Option B: Use start scripts (requires SSH to localhost)**  
Only if you have SSH running and passwordless login to localhost:

```bash
# If HADOOP_HOME is set:
"$HADOOP_HOME/sbin/start-dfs.sh"
"$HADOOP_HOME/sbin/start-yarn.sh"

# If not set, infer from hadoop on PATH:
HADOOP_SBIN="$(dirname "$(dirname "$(command -v hadoop)")")/sbin"
"$HADOOP_SBIN/start-dfs.sh"
"$HADOOP_SBIN/start-yarn.sh"
```

First-time setup only: format the NameNode once with `hdfs namenode -format` (see your Hadoop install docs). Verify HDFS is up: `hadoop fs -ls /` or `hdfs dfs -ls /`. If you see "Connection refused" on port 9000, HDFS is not running.

### 3. Make Scripts Executable

```bash
chmod +x scripts/compile.sh scripts/run_pipeline.sh scripts/test_sample.sh scripts/upload_wet_to_hdfs.sh
```

### 4. Prepare Data in HDFS (all ~99 WET files)

The pipeline processes **every file** in the input directory. Upload all WET files once:

```bash
# Option A: Use the upload script (from project root; uploads all data-* files)
./scripts/upload_wet_to_hdfs.sh
# Uses /user/$(whoami)/input/wet_files by default, or pass a path: ./scripts/upload_wet_to_hdfs.sh /user/you/input

# Option B: Manual upload (all 99 files from dataset-downloader)
hadoop fs -mkdir -p /user/$(whoami)/input/wet_files
hadoop fs -put dataset-downloader/downloaded_wet_files/data-* /user/$(whoami)/input/wet_files/

# Verify: should show ~99 files
hadoop fs -ls /user/$(whoami)/input/wet_files
```

## Compilation

From the project root:

```bash
./scripts/compile.sh
```

**Expected Output:**
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

## Execution

### Full Pipeline Execution

```bash
./scripts/run_pipeline.sh /user/$(whoami)/input/wet_files /user/$(whoami)/output
```

### Manual Execution (from project root)

```bash
hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver \
    /user/$(whoami)/input/wet_files \
    /user/$(whoami)/output
```

### Expected Runtime
- **Small dataset (< 1GB)**: 5-10 minutes
- **Medium dataset (5-10GB)**: 20-40 minutes
- **Full dataset (20GB)**: 45-90 minutes

*Times vary based on cluster size and configuration*

## Viewing Results

### Command Line

```bash
# Stage 1: Cleaned words
hadoop fs -cat /user/$(whoami)/output/stage1_cleaned/part-* | head -20

# Stage 2: Word counts (sorted)
hadoop fs -cat /user/$(whoami)/output/stage2_wordcount/part-* | sort -t$'\t' -k2 -rn | head -20

# Stage 3: Word length statistics
hadoop fs -cat /user/$(whoami)/output/stage3_wordlength/part-*

# Stage 4: Alphabet distribution
hadoop fs -cat /user/$(whoami)/output/stage4_alphabet/part-* | grep "^FIRST_"

# Stage 5: Top 1000 words
hadoop fs -cat /user/$(whoami)/output/stage5_topn/part-*

# Stage 6: Final analysis with summary
hadoop fs -cat /user/$(whoami)/output/stage6_final/part-*
```

### Download Results

```bash
# Download all results to local machine
hadoop fs -get /user/$(whoami)/output ./results

# View locally
ls -lh results/
```

## Output Format Examples

### Stage 1 Output (Cleaned Text)
```
hello
world
apache
hadoop
distributed
computing
```

### Stage 2 Output (Word Count)
```
the     15234
and     12456
data    8932
hadoop  7821
...
```

### Stage 3 Output (Word Length)
```
2   count=45678, avg_per_partition=456.78
3   count=123456, avg_per_partition=1234.56
4   count=234567, avg_per_partition=2345.67
...
```

### Stage 4 Output (Alphabet Distribution)
```
FIRST_a     12345
FIRST_b     8765
CHAR_a      56789
CHAR_b      45678
...
```

### Stage 5 Output (Top-N Words)
```
data        54321
system      43210
process     32109
...
```

### Stage 6 Output (Final Analysis)
```
algorithm   count=1234, length=9, category=LONG, topN=true
compute     count=2345, length=7, category=MEDIUM, topN=true
...
### SUMMARY_STATISTICS ###   total_word_occurrences=1234567, unique_words=45678, avg_frequency=27.04, most_common_length=7
```

## Troubleshooting

### Common Issues

1. **Compilation Errors**
   ```bash
   # Ensure HADOOP_HOME is set
   echo $HADOOP_HOME
   
   # If not set:
   export HADOOP_HOME=/path/to/hadoop
   export PATH=$PATH:$HADOOP_HOME/bin
   ```

2. **OutOfMemory Errors**
   ```bash
   # Increase mapper/reducer memory
   hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver \
       -D mapreduce.map.memory.mb=2048 \
       -D mapreduce.reduce.memory.mb=4096 \
       /user/$(whoami)/input/wet_files \
       /user/$(whoami)/output
   ```

3. **Input Path Does Not Exist**
   ```bash
   # Verify HDFS path
   hadoop fs -ls /user/$(whoami)/input/wet_files
   ```

4. **Permission Denied**
   ```bash
   # Check HDFS permissions
   hadoop fs -chmod -R 755 /user/$(whoami)/input
   ```

### Debugging

```bash
# View job logs
yarn logs -applicationId <application_id>

# Check HDFS health
hadoop fsck /

# Monitor job progress
# Access ResourceManager UI: http://localhost:8088
```

## Performance Optimization

### Tuning Parameters

```bash
# Adjust for your cluster size
hadoop jar build/webcrawl-pipeline.jar WebCrawlPipelineDriver \
    -D mapreduce.job.reduces=10 \
    -D mapreduce.map.memory.mb=2048 \
    -D mapreduce.reduce.memory.mb=4096 \
    -D mapreduce.task.io.sort.mb=512 \
    -D mapreduce.map.java.opts=-Xmx1638m \
    -D mapreduce.reduce.java.opts=-Xmx3276m \
    /user/$(whoami)/input/wet_files \
    /user/$(whoami)/output
```

### Best Practices

1. **Use Combiners**: Already implemented in Jobs 2, 3, and 4
2. **Compress Intermediate Data**: Enable compression
   ```bash
   -D mapreduce.map.output.compress=true \
   -D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
   ```
3. **Optimize Reducers**: Adjust based on data size
   - Small data (< 5GB): 3-5 reducers
   - Medium data (5-15GB): 8-12 reducers
   - Large data (> 15GB): 15-20 reducers

## Testing

### Sample Test with Small Dataset

```bash
# Create small test file
echo -e "hello world\nhello hadoop\nhadoop mapreduce\nworld data" > test.txt

# Upload to HDFS
hadoop fs -mkdir -p /user/$(whoami)/test_input
hadoop fs -put test.txt /user/$(whoami)/test_input/

# Run pipeline
./scripts/run_pipeline.sh /user/$(whoami)/test_input /user/$(whoami)/test_output

# Verify results
hadoop fs -cat /user/$(whoami)/test_output/stage6_final/part-*
```

## Academic Integrity Note

This implementation is designed as a learning resource. Students should:
- Understand each component's functionality
- Customize according to their specific requirements
- Test with their assigned data subset
- Document their own modifications and results

## Extension Ideas

1. **Add More Jobs**
   - N-gram analysis
   - TF-IDF calculation
   - Sentiment analysis

2. **Optimize Performance**
   - Custom partitioners
   - Data compression
   - Speculative execution tuning

3. **Enhanced Analytics**
   - Word co-occurrence patterns
   - Domain-specific filtering
   - Temporal analysis (if timestamps available)

## References

- Apache Hadoop Documentation: https://hadoop.apache.org/docs/
- MapReduce Design Patterns: https://www.oreilly.com/library/view/mapreduce-design-patterns/
- Hadoop: The Definitive Guide (O'Reilly)

## Support

For issues or questions:
1. Check Hadoop logs: `yarn logs -applicationId <app_id>`
2. Review MapReduce job history: ResourceManager UI
3. Verify HDFS status: `hadoop fsck /`

## Documentation

| Topic | Location |
|-------|----------|
| **Pipeline diagram (Mermaid)** | [docs/pipeline-architecture.mmd](docs/pipeline-architecture.mmd) |
| **Execution expectations (4 Mermaid diagrams)** | [docs/execution-expectations.md](docs/execution-expectations.md) |
| Architecture (text) | [docs/ARCHITECTURE_DIAGRAM.md](docs/ARCHITECTURE_DIAGRAM.md) |
| WET download (Common Crawl) | [docs/DATA_DOWNLOAD_GUIDE.md](docs/DATA_DOWNLOAD_GUIDE.md) |
| Quick start | [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) |
| File manifest | [docs/FILE_MANIFEST.md](docs/FILE_MANIFEST.md) |
| Command reference | [docs/QUICK_REFERENCE.md](docs/QUICK_REFERENCE.md) |
| Report template | [docs/PROJECT_REPORT.md](docs/PROJECT_REPORT.md) |

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.