# Hadoop MapReduce Web Crawl Pipeline - File Manifest

## Project Overview
Complete implementation of a 6-stage Hadoop MapReduce pipeline for processing 20GB of web crawl data.

## File Structure

Run all commands from the **project root**. Java sources are in `src/`, scripts in `scripts/`.

### Java Source Files (in src/)
```
src/WebCrawlPipelineDriver.java     - Main orchestrator for all 6 jobs
src/TextCleaningJob.java            - Stage 1: Text cleaning and normalization
src/WordCountJob.java               - Stage 2: Word count aggregation
src/WordLengthStatsJob.java         - Stage 3: Word length statistics
src/AlphabetDistributionJob.java    - Stage 4: Alphabet distribution analysis
src/TopNWordsJob.java               - Stage 5: Top-N frequent words
src/FinalAnalysisJob.java           - Stage 6: Final filtered ranking
```

### Shell Scripts (in scripts/)
```
scripts/compile.sh                  - Compilation script for building JAR
scripts/run_pipeline.sh             - Execution script (processes all files in input dir)
scripts/upload_wet_to_hdfs.sh       - Upload all ~99 WET files to HDFS
scripts/test_sample.sh              - Testing script with sample data
```

### Documentation
```
README.md                           - Project overview and usage (repository root)
docs/README.md                      - Documentation index
docs/pipeline-architecture.mmd     - Mermaid pipeline diagram (detailed; render with Mermaid)
docs/execution-expectations.md      - Four Mermaid diagrams: WET processing, six jobs, distributed behaviour, Driver orchestration
docs/ARCHITECTURE_DIAGRAM.md        - Text-based pipeline flow and job details
docs/DATA_DOWNLOAD_GUIDE.md         - WET download from Common Crawl
docs/GETTING_STARTED.md             - Quick start and setup
docs/FILE_MANIFEST.md               - This file
docs/QUICK_REFERENCE.md             - Command reference
docs/PROJECT_REPORT.md              - Report template for submission
```

## File Sizes
```
Java source files:     ~51 KB total
Shell scripts:         ~9 KB total
Documentation:         ~33 KB total
Total project size:    ~93 KB (before compilation)
```

## Getting Started (from project root)

1. **Extract/navigate** to project root (hadoop-webcrawl-pipeline/)
2. **Make scripts executable**: `chmod +x scripts/*.sh`
3. **Compile**: `./scripts/compile.sh`
4. **Test**: `./scripts/test_sample.sh`
5. **Run**: `./scripts/run_pipeline.sh <input> <output>`

## File Dependencies

### Compilation Order
All Java files can be compiled together. Dependencies:
- All jobs depend on Hadoop libraries (provided by classpath)
- Driver depends on all job classes
- No circular dependencies

### Execution Order (Automatic via Driver)
```
Driver
  ├── Job 1 (TextCleaningJob)
  │     ├── Output → Job 2 input
  │     ├── Output → Job 3 input
  │     └── Output → Job 4 input
  ├── Job 2 (WordCountJob)
  │     ├── Output → Job 5 input
  │     └── Output → Job 6 input
  ├── Job 3 (WordLengthStatsJob)
  ├── Job 4 (AlphabetDistributionJob)
  ├── Job 5 (TopNWordsJob)
  │     └── Output → Job 6 input
  └── Job 6 (FinalAnalysisJob)
```

## Required External Dependencies
- Hadoop 2.x or 3.x
- JDK 8 or higher
- HDFS configured and running
- YARN (optional, for cluster mode)

## Output After Compilation
```
build/
├── classes/
│   ├── WebCrawlPipelineDriver.class
│   ├── WebCrawlPipelineDriver$1.class
│   ├── TextCleaningJob.class
│   ├── TextCleaningJob$CleaningMapper.class
│   ├── TextCleaningJob$CleaningReducer.class
│   ├── WordCountJob.class
│   ├── WordCountJob$WordCountMapper.class
│   ├── WordCountJob$WordCountCombiner.class
│   ├── WordCountJob$WordCountReducer.class
│   ├── [... and all other class files ...]
│   └── Total: ~35 class files
└── webcrawl-pipeline.jar (~50 KB)
```

## Expected Runtime (approximate)
- Compilation: 10-30 seconds
- Test execution: 1-2 minutes
- Full pipeline (20GB): 45-90 minutes
  - Stage 1: 15-30 minutes
  - Stage 2: 10-20 minutes
  - Stage 3: 5-10 minutes
  - Stage 4: 5-10 minutes
  - Stage 5: 2-5 minutes
  - Stage 6: 3-8 minutes

## Disk Space Requirements
- Source files: <1 MB
- Compiled JAR: ~50 KB
- Input data (WET files): ~20 GB
- Intermediate data: ~5-8 GB
- Final output: ~500 MB
- Total HDFS space needed: ~25-30 GB
- Local space for downloaded results: ~500 MB

## Network Requirements
- For distributed execution: High-speed cluster network
- For standalone: Local file system access
- No internet required for execution

## Customization Points

### Easy to Modify
1. **Top-N value**: Change `TOP_N` constant in TopNWordsJob.java (line 38)
2. **Stop words**: Modify `STOP_WORDS` set in FinalAnalysisJob.java (line 32)
3. **Word length categories**: Adjust `categorizeWord()` in FinalAnalysisJob.java (line 158)
4. **Minimum word length**: Change filter in TextCleaningJob.java (line 79)

### Advanced Customization
1. **Number of reducers**: Modify `mapreduce.job.reduces` parameter
2. **Memory allocation**: Adjust memory parameters in scripts/run_pipeline.sh
3. **Compression**: Enable in Hadoop configuration
4. **Custom partitioning**: Implement custom Partitioner class

## Testing Checklist

Before submission, verify:
- [ ] All 13 files present
- [ ] Scripts have execute permissions
- [ ] Compilation succeeds without errors
- [ ] Test script runs successfully
- [ ] All 6 stages execute sequentially
- [ ] Output files generated in HDFS
- [ ] Results can be viewed/downloaded
- [ ] Documentation is complete

## Troubleshooting Quick Links

**Compilation fails**: See [QUICK_REFERENCE.md](QUICK_REFERENCE.md) → Compilation
**Job fails**: See [../README.md](../README.md) → Troubleshooting
**Performance issues**: See [PROJECT_REPORT.md](PROJECT_REPORT.md) → Performance Optimization
**HDFS errors**: See [QUICK_REFERENCE.md](QUICK_REFERENCE.md) → Troubleshooting

## Version Information
- **Version**: 1.0
- **Date**: February 2026
- **Hadoop API**: 2.x/3.x compatible
- **Java version**: 8+ compatible

## Changelog
- v1.0 (2026-02): Initial release
  - Complete 6-stage pipeline
  - Full documentation
  - Test scripts included

## License
Educational use only. Provided as-is for learning purposes.

## Contact
For issues or questions, refer to documentation or course instructor.

---

**Quick Commands** (from project root):
```bash
# Compile
./scripts/compile.sh

# Test
./scripts/test_sample.sh

# Run
./scripts/run_pipeline.sh /input /output

# View results
hadoop fs -cat /output/stage6_final/part-* | head -20
```

**Remember**: Always test with small data first before processing the full 20GB dataset!