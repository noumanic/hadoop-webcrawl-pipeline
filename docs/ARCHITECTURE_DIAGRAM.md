# Hadoop MapReduce Pipeline — Visual Architecture

For a **detailed Mermaid diagram** (driver, all stages, HDFS paths, mapper/reducer names, and legend), see **[pipeline-architecture.mmd](pipeline-architecture.mmd)**. Render it with [Mermaid Live](https://mermaid.live) or a Mermaid-capable editor.

---

## Complete Pipeline Flow Diagram (Text)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           WET FILES INPUT (~20GB)                           │
│         [100 WET files (~200–250MB each compressed), plain text]             │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   │ HDFS: /input/wet_files/
                                   │
                                   ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                    STAGE 1: TEXT CLEANING & NORMALIZATION                    ║
║──────────────────────────────────────────────────────────────────────────────║
║  Mapper: CleaningMapper                                                      ║
║    • Remove URLs, emails, HTML entities                                      ║
║    • Convert to lowercase                                                    ║
║    • Strip non-alphabetic characters                                         ║
║    • Filter words < 2 characters                                             ║
║    • Emit: (cleaned_word, empty)                                             ║
║                                                                              ║
║  Reducer: CleaningReducer                                                    ║
║    • Deduplicate words                                                       ║
║    • Emit: (word, empty)                                                     ║
╚══════════════════════════════════════════════════════════════════════════════╝
                                   │
                                   │ HDFS: /output/stage1_cleaned/
                                   │ Format: word\n
                                   │
                    ┌──────────────┼──────────────┬────────────┐
                    │              │              │            │
                    ▼              ▼              ▼            │
         ╔═══════════════╗ ╔═══════════════╗ ╔══════════════╗  │
         ║   STAGE 2:    ║ ║   STAGE 3:    ║ ║  STAGE 4:    ║  │
         ║ WORD COUNT    ║ ║ WORD LENGTH   ║ ║  ALPHABET    ║  │
         ║ AGGREGATION   ║ ║  STATISTICS   ║ ║ DISTRIBUTION ║  │
         ╚═══════════════╝ ╚═══════════════╝ ╚══════════════╝  │
                    │              │              │            │
                    ▼              ▼              ▼            │
         ┌─────────────────────────────────────────────────┐   │
         │ HDFS Outputs:                                   │   │
         │ • stage2: word\tcount                           │   │
         │ • stage3: length\tstatistics                    │   │
         │ • stage4: letter_type\tcount                    │   │
         └─────────────────────────────────────────────────┘   │
                    │                                          │
                    │                                          │
                    ▼                                          │
         ╔═══════════════════════════════════════════════╗     │
         ║          STAGE 5: TOP-N WORDS                 ║     │
         ║───────────────────────────────────────────────║     │
         ║  Mapper: TopNMapper                           ║     │
         ║    • Read word counts from Stage 2            ║     │
         ║    • Swap to (count, word)                    ║     │
         ║    • Enable sorting by frequency              ║     │
         ║                                               ║     │
         ║  Custom Comparator: DescendingIntComparator   ║     │
         ║    • Sort counts descending                   ║     │
         ║                                               ║     │
         ║  Reducer: TopNReducer (single reducer)        ║     │
         ║    • Extract top 1000 words                   ║     │
         ║    • Emit: (word, count)                      ║     │
         ╚═══════════════════════════════════════════════╝     │
                    │                                          │
                    │ HDFS: /output/stage5_topn/               │
                    │ Format: word\tcount (sorted desc)        │
                    │                                          │
                    └────────────────┬─────────────────────────┘
                                     │
                                     ▼
         ╔═══════════════════════════════════════════════════════════════╗
         ║              STAGE 6: FINAL FILTERED RANKING                  ║
         ║───────────────────────────────────────────────────────────────║
         ║  Multiple Inputs:                                             ║
         ║    Input 1: stage2_wordcount/ → WordCountDataMapper           ║
         ║    Input 2: stage5_topn/      → TopNDataMapper                ║
         ║                                                               ║
         ║  WordCountDataMapper:                                         ║
         ║    • Filter stop words                                        ║
         ║    • Emit: (word, "COUNT:X")                                  ║
         ║                                                               ║
         ║  TopNDataMapper:                                              ║
         ║    • Mark top-N words                                         ║
         ║    • Emit: (word, "TOPN:true")                                ║
         ║                                                               ║
         ║  FinalAnalysisReducer:                                        ║
         ║    • Combine data from both sources                           ║
         ║    • Filter: topN OR count >= 10                              ║
         ║    • Categorize: SHORT/MEDIUM/LONG/VERY_LONG                  ║
         ║    • Calculate analytics                                      ║
         ║    • Emit: (word, "count=X, length=Y, category=Z, topN=bool") ║
         ║    • In cleanup: emit summary statistics                      ║
         ╚═══════════════════════════════════════════════════════════════╝
                                     │
                                     │ HDFS: /output/stage6_final/
                                     │
                                     ▼
         ┌───────────────────────────────────────────────────────────────┐
         │                      FINAL RESULTS                            │
         │───────────────────────────────────────────────────────────────│
         │  • Filtered word rankings with analytics                      │
         │  • Summary statistics:                                        │
         │    - Total word occurrences                                   │
         │    - Unique words count                                       │
         │    - Average frequency                                        │
         │    - Most common word length                                  │
         └───────────────────────────────────────────────────────────────┘
```

## Data Flow Detail

```
Input Data → [20GB WET files]
    ↓
Stage 1 → [~8GB cleaned words]
    ├─→ Stage 2 → [~500MB word counts]
    │       ├─→ Stage 5 → [~50KB top-1000]
    │       │       └─→ Stage 6 → [~200KB final analysis]
    │       └─→ Stage 6
    ├─→ Stage 3 → [~10KB length stats]
    └─→ Stage 4 → [~5KB alphabet dist]

Total Processing Time: 45-90 minutes
Total Output Size: ~9GB (including intermediate data)
Final Results: ~200KB
```

## MapReduce Job Details

### Stage 1: Text Cleaning
```
Input:     20GB raw text
Mappers:   ~200 (1 per file)
Reducers:  10
Output:    ~8GB cleaned words
Runtime:   15-30 minutes
```

### Stage 2: Word Count
```
Input:     8GB cleaned words
Mappers:   ~80
Reducers:  10
Combiner:  Yes (60% reduction)
Output:    ~500MB word counts
Runtime:   10-20 minutes
```

### Stage 3: Word Length Stats
```
Input:     8GB cleaned words
Mappers:   ~80
Reducers:  5
Output:    ~10KB statistics
Runtime:   5-10 minutes
```

### Stage 4: Alphabet Distribution
```
Input:     8GB cleaned words
Mappers:   ~80
Reducers:  5
Output:    ~5KB letter counts
Runtime:   5-10 minutes
```

### Stage 5: Top-N Words
```
Input:     500MB word counts
Mappers:   ~10
Reducers:  1 (required for global top-N)
Output:    ~50KB top-1000 words
Runtime:   2-5 minutes
```

### Stage 6: Final Analysis
```
Input:     500MB + 50KB (two sources)
Mappers:   ~12 (2 types)
Reducers:  5
Output:    ~200KB filtered results
Runtime:   3-8 minutes
```

## Driver Execution Flow

```
main()
  ├─→ Validate arguments (input path, output path)
  ├─→ Initialize Configuration
  ├─→ Get FileSystem instance
  │
  ├─→ [1] Run TextCleaningJob
  │     ├─ Check/delete existing output
  │     ├─ Execute job
  │     └─ Wait for completion → Exit if failed
  │
  ├─→ [2] Run WordCountJob
  │     ├─ Input: stage1_cleaned
  │     ├─ Execute job
  │     └─ Wait for completion → Exit if failed
  │
  ├─→ [3] Run WordLengthStatsJob
  │     ├─ Input: stage1_cleaned
  │     ├─ Execute job (parallel to Stage 2)
  │     └─ Wait for completion → Exit if failed
  │
  ├─→ [4] Run AlphabetDistributionJob
  │     ├─ Input: stage1_cleaned
  │     ├─ Execute job (parallel to Stages 2-3)
  │     └─ Wait for completion → Exit if failed
  │
  ├─→ [5] Run TopNWordsJob
  │     ├─ Input: stage2_wordcount
  │     ├─ Execute job (depends on Stage 2)
  │     └─ Wait for completion → Exit if failed
  │
  ├─→ [6] Run FinalAnalysisJob
  │     ├─ Input: stage2_wordcount + stage5_topn
  │     ├─ Execute job (depends on Stages 2 and 5)
  │     └─ Wait for completion → Exit if failed
  │
  └─→ Print success message with output locations
```

## Key Design Features

### 1. Pipeline Architecture
- **Sequential execution** ensures data dependencies are met
- **Automatic cleanup** of intermediate directories
- **Error propagation** stops pipeline on any failure
- **Progress logging** shows current stage

### 2. Distributed Processing
- **Horizontal scaling** across multiple nodes
- **Data locality** optimization via HDFS
- **Fault tolerance** via task retry mechanism
- **Load balancing** through dynamic task allocation

### 3. Optimization Techniques
- **Combiners** reduce network traffic (Jobs 2, 3, 4)
- **Single reducer** for global operations (Job 5)
- **Multiple inputs** for data fusion (Job 6)
- **Custom comparators** for specialized sorting (Job 5)

### 4. Data Quality
- **Stop word filtering** removes common terms
- **Length filtering** removes noise (< 2 chars)
- **Deduplication** in early stages
- **Categorization** for meaningful grouping

## Resource Utilization

```
┌─────────────────────────────────────────────────────┐
│                 Cluster Resources                   │
├─────────────────────────────────────────────────────┤
│  CPU Usage:                                         │
│    Peak:    ~80% (during Stage 1 mapping)           │
│    Average: ~50% (across all stages)                │
│                                                     │
│  Memory Usage:                                      │
│    Mappers:  2GB per task                           │
│    Reducers: 4GB per task                           │
│    Total:    ~80GB peak                             │
│                                                     │
│  Disk I/O:                                          │
│    Read:     ~350 MB/s (Stage 1)                    │
│    Write:    ~150 MB/s (Stage 1)                    │
│    Network:  ~100 MB/s (shuffle phase)              │
│                                                     │
│  HDFS:                                              │
│    Input:    20GB                                   │
│    Temp:     13GB (intermediate data)               │
│    Output:   0.7GB (final results)                  │
│    Total:    ~34GB (with replication factor 3)      │
└─────────────────────────────────────────────────────┘
```

## Success Metrics

✓ All 6 stages execute sequentially
✓ No job failures or task retries
✓ Output files generated in all stages
✓ Summary statistics calculated correctly
✓ Processing time within expected range
✓ Memory usage stays within limits
✓ HDFS capacity sufficient for all data

---

**Note**: This diagram represents the logical flow. Actual execution may vary based on cluster configuration and data characteristics.