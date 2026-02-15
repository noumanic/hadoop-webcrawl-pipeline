import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;

/**
 * Main Driver class for the Web Crawl Processing Pipeline
 * Executes 6 sequential MapReduce jobs to process WET files
 */
public class WebCrawlPipelineDriver extends Configured implements Tool {
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WebCrawlPipelineDriver <input_path> <output_base_path>");
            return -1;
        }
        
        String inputPath = args[0];
        String outputBasePath = args[1];
        
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        
        System.out.println("========================================");
        System.out.println("WEB CRAWL PROCESSING PIPELINE STARTING");
        System.out.println("========================================");
        System.out.println("Input path:  " + inputPath);
        System.out.println("Output path: " + outputBasePath);
        System.out.println("(All files in the input directory will be processed.)");
        
        // Define intermediate paths
        String job1Output = outputBasePath + "/stage1_cleaned";
        String job2Output = outputBasePath + "/stage2_wordcount";
        String job3Output = outputBasePath + "/stage3_wordlength";
        String job4Output = outputBasePath + "/stage4_alphabet";
        String job5Output = outputBasePath + "/stage5_topn";
        String job6Output = outputBasePath + "/stage6_final";
        
        // Job 1: Text Cleaning and Normalization
        System.out.println("\n[Stage 1/6] Starting Text Cleaning and Normalization...");
        if (fs.exists(new Path(job1Output))) {
            fs.delete(new Path(job1Output), true);
        }
        int job1Result = ToolRunner.run(conf, new TextCleaningJob(), 
            new String[]{inputPath, job1Output});
        if (job1Result != 0) {
            System.err.println("Job 1 failed!");
            return job1Result;
        }
        System.out.println("[Stage 1/6] Completed Successfully!");
        
        // Job 2: Word Count Aggregation
        System.out.println("\n[Stage 2/6] Starting Word Count Aggregation...");
        if (fs.exists(new Path(job2Output))) {
            fs.delete(new Path(job2Output), true);
        }
        int job2Result = ToolRunner.run(conf, new WordCountJob(), 
            new String[]{job1Output, job2Output});
        if (job2Result != 0) {
            System.err.println("Job 2 failed!");
            return job2Result;
        }
        System.out.println("[Stage 2/6] Completed Successfully!");
        
        // Job 3: Word Length Statistics
        System.out.println("\n[Stage 3/6] Starting Word Length Statistics...");
        if (fs.exists(new Path(job3Output))) {
            fs.delete(new Path(job3Output), true);
        }
        int job3Result = ToolRunner.run(conf, new WordLengthStatsJob(), 
            new String[]{job1Output, job3Output});
        if (job3Result != 0) {
            System.err.println("Job 3 failed!");
            return job3Result;
        }
        System.out.println("[Stage 3/6] Completed Successfully!");
        
        // Job 4: Alphabet Distribution Analysis
        System.out.println("\n[Stage 4/6] Starting Alphabet Distribution Analysis...");
        if (fs.exists(new Path(job4Output))) {
            fs.delete(new Path(job4Output), true);
        }
        int job4Result = ToolRunner.run(conf, new AlphabetDistributionJob(), 
            new String[]{job1Output, job4Output});
        if (job4Result != 0) {
            System.err.println("Job 4 failed!");
            return job4Result;
        }
        System.out.println("[Stage 4/6] Completed Successfully!");
        
        // Job 5: Top-N Frequent Words Identification
        System.out.println("\n[Stage 5/6] Starting Top-N Frequent Words Identification...");
        if (fs.exists(new Path(job5Output))) {
            fs.delete(new Path(job5Output), true);
        }
        int job5Result = ToolRunner.run(conf, new TopNWordsJob(), 
            new String[]{job2Output, job5Output});
        if (job5Result != 0) {
            System.err.println("Job 5 failed!");
            return job5Result;
        }
        System.out.println("[Stage 5/6] Completed Successfully!");
        
        // Job 6: Final Filtered Ranking and Analytical Summary
        System.out.println("\n[Stage 6/6] Starting Final Filtered Ranking...");
        if (fs.exists(new Path(job6Output))) {
            fs.delete(new Path(job6Output), true);
        }
        int job6Result = ToolRunner.run(conf, new FinalAnalysisJob(), 
            new String[]{job2Output, job5Output, job6Output});
        if (job6Result != 0) {
            System.err.println("Job 6 failed!");
            return job6Result;
        }
        System.out.println("[Stage 6/6] Completed Successfully!");
        
        System.out.println("\n========================================");
        System.out.println("PIPELINE COMPLETED SUCCESSFULLY!");
        System.out.println("========================================");
        System.out.println("Output locations:");
        System.out.println("  Stage 1 (Cleaned Text): " + job1Output);
        System.out.println("  Stage 2 (Word Counts): " + job2Output);
        System.out.println("  Stage 3 (Word Length Stats): " + job3Output);
        System.out.println("  Stage 4 (Alphabet Distribution): " + job4Output);
        System.out.println("  Stage 5 (Top-N Words): " + job5Output);
        System.out.println("  Stage 6 (Final Analysis): " + job6Output);
        
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), 
            new WebCrawlPipelineDriver(), args);
        System.exit(exitCode);
    }
}