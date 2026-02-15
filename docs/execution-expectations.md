# Execution Expectations — Detailed Diagrams

This document illustrates the four execution expectations for the Hadoop Web Crawl Processing Pipeline, each with a **detailed Mermaid diagram**. Render the diagrams on GitHub (native Mermaid support), in [Mermaid Live](https://mermaid.live), or in VS Code with a Mermaid extension.

---

## 1. Successful Processing of Assigned WET Files

Assigned WET files must be fully and correctly processed: from availability on disk through HDFS upload to consumption by the pipeline, with no files skipped and no silent failures.

```mermaid
flowchart TB
    subgraph ASSIGNED["Assigned dataset"]
        A1["~99 WET files<br/>data-* naming"]
        A2["~20 GB total<br/>200–250 MB per file"]
        A3["Common Crawl format<br/>WARC headers + plain text"]
    end

    subgraph VERIFY["Verification"]
        V1["Count files<br/>ls data-*"]
        V2["Check HDFS path<br/>hadoop fs -ls input/"]
        V3["Run script reports<br/>'N file(s) will be processed'"]
    end

    subgraph UPLOAD["Upload to HDFS"]
        U1["upload_wet_to_hdfs.sh<br/>or hadoop fs -put"]
        U2["Single directory<br/>/user/&lt;user&gt;/input/wet_files/"]
        U3["FileInputFormat.addInputPath<br/>processes every file in dir"]
    end

    subgraph PROCESS["Pipeline processing"]
        P1["Job 1: TextCleaningJob<br/>Reads all splits from all files"]
        P2["Each file → one or more<br/>input splits"]
        P3["Success: all splits read<br/>no IOException, no skip"]
    end

    subgraph SUCCESS["Success criteria"]
        S1["All assigned files present in HDFS"]
        S2["Input path contains expected count"]
        S3["All six jobs complete with exit code 0"]
        S4["Stage outputs contain data from all files"]
    end

    A1 --> V1
    A2 --> V1
    A3 --> V1
    V1 --> V2 --> V3
    V3 --> U1 --> U2 --> U3
    U3 --> P1
    P2 --> P1
    P1 --> P3
    P3 --> S1 --> S2 --> S3 --> S4

    style ASSIGNED fill:#E3F2FD
    style VERIFY fill:#FFF3E0
    style UPLOAD fill:#E8F5E9
    style PROCESS fill:#F3E5F5
    style SUCCESS fill:#C8E6C9
```

**Summary:** Assigned WET files are verified, uploaded to one HDFS directory, and processed in full by the pipeline. The run script reports the input file count; the Driver uses `FileInputFormat.addInputPath(conf, inputDir)`, so every file in that directory is included in the job input.

---

## 2. Execution of All Six MapReduce Jobs

All six MapReduce jobs must execute in order; each job must complete successfully before the next starts. Failure of any job stops the pipeline and returns a non-zero exit code.

```mermaid
flowchart LR
    subgraph DRIVER["WebCrawlPipelineDriver"]
        D0["run(args)"]
    end

    subgraph JOBS["Six MapReduce jobs (sequential)"]
        J1["Job 1<br/>TextCleaningJob<br/>Input: raw WET<br/>Output: stage1_cleaned"]
        J2["Job 2<br/>WordCountJob<br/>Input: stage1<br/>Output: stage2_wordcount"]
        J3["Job 3<br/>WordLengthStatsJob<br/>Input: stage1<br/>Output: stage3_wordlength"]
        J4["Job 4<br/>AlphabetDistributionJob<br/>Input: stage1<br/>Output: stage4_alphabet"]
        J5["Job 5<br/>TopNWordsJob<br/>Input: stage2<br/>Output: stage5_topn"]
        J6["Job 6<br/>FinalAnalysisJob<br/>Input: stage2 + stage5<br/>Output: stage6_final"]
    end

    subgraph CHECK["Per-job checks"]
        C1["fs.exists(output)?<br/>→ delete"]
        C2["ToolRunner.run(conf, job, args)"]
        C3["exitCode != 0?<br/>→ return exitCode"]
    end

    D0 --> J1
    J1 --> C1 --> C2 --> C3
    C3 -->|"0"| J2
    J2 --> C1 --> C2 --> C3
    C3 -->|"0"| J3
    J3 --> C1 --> C2 --> C3
    C3 -->|"0"| J4
    J4 --> C1 --> C2 --> C3
    C3 -->|"0"| J5
    J5 --> C1 --> C2 --> C3
    C3 -->|"0"| J6
    J6 --> C1 --> C2 --> C3
    C3 -->|"0"| END["Print summary<br/>return 0"]

    style DRIVER fill:#37474F,color:#FFF
    style JOBS fill:#E8F5E9
    style CHECK fill:#FFF3E0
    style END fill:#C8E6C9
```

**Execution order (enforced by Driver):**

| Step | Job class              | Input              | Output          |
|------|------------------------|--------------------|-----------------|
| 1    | TextCleaningJob        | WET path           | stage1_cleaned  |
| 2    | WordCountJob           | stage1_cleaned     | stage2_wordcount|
| 3    | WordLengthStatsJob     | stage1_cleaned     | stage3_wordlength |
| 4    | AlphabetDistributionJob| stage1_cleaned    | stage4_alphabet |
| 5    | TopNWordsJob           | stage2_wordcount   | stage5_topn     |
| 6    | FinalAnalysisJob       | stage2 + stage5    | stage6_final    |

Each job runs only after the previous one has finished successfully (`job.waitForCompletion(true)`). The Driver does not start Job 2 until Job 1 returns 0, and so on for all six jobs.

---

## 3. Distributed Processing Behaviour

The pipeline exhibits distributed processing: multiple mappers and reducers run on cluster nodes, data is read from HDFS with locality where possible, and the shuffle phase moves intermediate data across the network. No single node performs all work.

```mermaid
flowchart TB
    subgraph HDFS["HDFS layer"]
        NN["NameNode<br/>metadata"]
        DN1["DataNode 1<br/>blocks"]
        DN2["DataNode 2<br/>blocks"]
        DN3["DataNode 3<br/>blocks"]
        NN --> DN1 & DN2 & DN3
    end

    subgraph YARN["YARN layer"]
        RM["ResourceManager<br/>scheduling"]
        NM1["NodeManager 1"]
        NM2["NodeManager 2"]
        NM3["NodeManager 3"]
        RM --> NM1 & NM2 & NM3
    end

    subgraph JOB1["Job 1 — Map phase (distributed)"]
        M1A["Mapper 1<br/>DataNode 1"]
        M1B["Mapper 2<br/>DataNode 2"]
        M1C["Mapper 3<br/>DataNode 3"]
    end

    subgraph SHUFFLE["Shuffle &amp; Sort"]
        S1["Partition by key<br/>Transfer over network"]
        S2["Sort at reducer<br/>Group by key"]
    end

    subgraph REDUCE["Reduce phase (distributed)"]
        R1["Reducer 1<br/>NodeManager 1"]
        R2["Reducer 2<br/>NodeManager 2"]
    end

    subgraph BEHAVIOUR["Distributed behaviour"]
        B1["Data locality: map tasks prefer same node as block"]
        B2["Parallel maps: many mappers across nodes"]
        B3["Parallel reduces: multiple reducers"]
        B4["Combiners reduce network (Jobs 2, 3, 4)"]
        B5["Single reducer only where required (Job 5)"]
    end

    DN1 --> M1A
    DN2 --> M1B
    DN3 --> M1C
    M1A & M1B & M1C --> S1 --> S2 --> R1 & R2
    NM1 --> M1A
    NM2 --> M1B
    NM3 --> M1C
    RM --> JOB1
    SHUFFLE --> BEHAVIOUR

    style HDFS fill:#E3F2FD
    style YARN fill:#FFF3E0
    style JOB1 fill:#E8F5E9
    style SHUFFLE fill:#FCE4EC
    style REDUCE fill:#E8F5E9
    style BEHAVIOUR fill:#F3E5F5
```

**How distribution is achieved:**

- **Input splits:** Each WET file (or block) becomes one or more input splits; each split is assigned to one map task.
- **Map tasks:** Many map tasks run in parallel on different nodes; Hadoop schedules them close to the data when possible.
- **Combiners:** Jobs 2, 3, and 4 use combiners so that partial aggregation happens on the map side, reducing data sent to reducers.
- **Shuffle:** Key-value pairs are partitioned (e.g. by hash of key), sent over the network, and sorted at the reducer.
- **Reduce tasks:** Multiple reducers (configurable; Job 5 uses one by design) run in parallel and write to HDFS.
- **No single point of work:** No single node runs all mappers or all reducers; the workload is spread across the cluster.

---

## 4. Proper Pipeline Orchestration via Driver Class

The Driver class is the single entry point that orchestrates the entire pipeline: it validates arguments, configures the job sequence, manages HDFS paths, runs each job, and enforces success/failure semantics.

```mermaid
sequenceDiagram
    participant User
    participant Main as main()
    participant Driver as WebCrawlPipelineDriver
    participant FS as FileSystem
    participant J1 as TextCleaningJob
    participant J2 as WordCountJob
    participant J3 as WordLengthStatsJob
    participant J4 as AlphabetDistributionJob
    participant J5 as TopNWordsJob
    participant J6 as FinalAnalysisJob

    User->>Main: hadoop jar ... WebCrawlPipelineDriver input output
    Main->>Driver: ToolRunner.run(conf, driver, args)
    Driver->>Driver: args.length != 2 ? return -1
    Driver->>FS: FileSystem.get(conf)
    Driver->>Driver: Print "PIPELINE STARTING"<br/>input path, output path

    rect rgb(232, 245, 233)
        Driver->>Driver: job1Output = outputBasePath + "/stage1_cleaned"
        Driver->>FS: fs.exists(job1Output)? fs.delete(..., true)
        Driver->>J1: ToolRunner.run(conf, TextCleaningJob, [input, job1Output])
        J1-->>Driver: exitCode
        alt exitCode != 0
            Driver->>User: "Job 1 failed!" ; return exitCode
        end
    end

    rect rgb(227, 242, 253)
        Driver->>FS: fs.exists(job2Output)? fs.delete(...)
        Driver->>J2: ToolRunner.run(conf, WordCountJob, [job1Output, job2Output])
        J2-->>Driver: exitCode
        alt exitCode != 0
            Driver->>User: "Job 2 failed!" ; return exitCode
        end
    end

    Driver->>J3: ToolRunner.run(WordLengthStatsJob, ...)
    J3-->>Driver: exitCode
    Driver->>J4: ToolRunner.run(AlphabetDistributionJob, ...)
    J4-->>Driver: exitCode
    Driver->>J5: ToolRunner.run(TopNWordsJob, ...)
    J5-->>Driver: exitCode
    Driver->>J6: ToolRunner.run(FinalAnalysisJob, [job2, job5, job6Output])
    J6-->>Driver: exitCode

    alt any exitCode != 0
        Driver->>User: "Job N failed!" ; return exitCode
    end

    Driver->>Driver: Print "PIPELINE COMPLETED SUCCESSFULLY"<br/>Print all output locations
    Driver->>Main: return 0
    Main->>User: System.exit(0)
```

**Orchestration responsibilities of the Driver:**

| Responsibility | Implementation |
|----------------|----------------|
| **Argument validation** | `args.length != 2` → print usage, return -1 |
| **Configuration** | `getConf()` passed to each job via ToolRunner |
| **HDFS path setup** | Fixed names: stage1_cleaned, stage2_wordcount, … stage6_final |
| **Output cleanup** | Before each job: `if (fs.exists(output)) fs.delete(output, true)` |
| **Sequential execution** | Jobs run one after another; next job only if previous returns 0 |
| **Failure handling** | On non-zero return: print "Job N failed!", return that code |
| **Summary** | On success: print all stage output paths |

The Driver does not implement map/reduce logic; it only invokes the six job classes and controls the workflow. Pipeline orchestration is therefore centralized and explicit in `WebCrawlPipelineDriver.run(String[] args)`.
