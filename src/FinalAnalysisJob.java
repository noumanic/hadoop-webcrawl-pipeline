import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.*;

/**
 * Job 6: Final Filtered Ranking and Analytical Summary
 * 
 * Purpose: Combine results from previous jobs to produce final analytics
 * - Filter out common stop words
 * - Analyze word characteristics (length, letter distribution)
 * - Generate comprehensive summary statistics
 * 
 * Input: Word counts from Job 2 AND Top-N words from Job 5
 * Output: Filtered rankings with analytical insights
 */
public class FinalAnalysisJob extends Configured implements Tool {
    
    // Common English stop words to filter out
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
        "the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
        "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
        "this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
        "or", "an", "will", "my", "one", "all", "would", "there", "their",
        "what", "so", "up", "out", "if", "about", "who", "get", "which", "go",
        "me", "when", "make", "can", "like", "time", "no", "just", "him", "know"
    ));
    
    /**
     * Mapper for word count data (Job 2 output)
     */
    public static class WordCountDataMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text word = new Text();
        private Text outputValue = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString().trim();
            
            if (line.isEmpty()) {
                return;
            }
            
            // Job 2 output format: word\tcount (tab-separated)
            String[] parts = line.split("\\t", 2);
            if (parts.length >= 2) {
                String wordStr = parts[0];
                String count = parts[1].trim();
                
                // Skip stop words
                if (STOP_WORDS.contains(wordStr.toLowerCase())) {
                    return;
                }
                
                word.set(wordStr);
                outputValue.set("COUNT:" + count);
                context.write(word, outputValue);
            }
        }
    }
    
    /**
     * Mapper for top-N data (Job 5 output)
     */
    public static class TopNDataMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text word = new Text();
        private Text outputValue = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString().trim();
            
            if (line.isEmpty()) {
                return;
            }
            
            // Job 5 output format: word\tcount (tab-separated)
            String[] parts = line.split("\\t", 2);
            if (parts.length >= 1 && !parts[0].isEmpty()) {
                String wordStr = parts[0];
                word.set(wordStr);
                outputValue.set("TOPN:true");
                context.write(word, outputValue);
            }
        }
    }
    
    /**
     * Reducer: Combine data and generate analytical summary
     */
    public static class FinalAnalysisReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();
        private long totalWords = 0;
        private long totalUniqueWords = 0;
        private Map<Integer, Integer> lengthDistribution = new TreeMap<>();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            int count = 0;
            boolean isTopN = false;
            
            // Process all values for this word
            for (Text val : values) {
                String valStr = val.toString();
                
                if (valStr.startsWith("COUNT:")) {
                    count = Integer.parseInt(valStr.substring(6));
                } else if (valStr.startsWith("TOPN:")) {
                    isTopN = true;
                }
            }
            
            // Only process words that appear in both datasets or have significant count
            if (count > 0 && (isTopN || count >= 10)) {
                String word = key.toString();
                int wordLength = word.length();
                
                // Update statistics
                totalWords += count;
                totalUniqueWords++;
                lengthDistribution.put(wordLength, 
                    lengthDistribution.getOrDefault(wordLength, 0) + 1);
                
                // Calculate analytics
                String category = categorizeWord(word);
                String analysis = String.format(
                    "count=%d, length=%d, category=%s, topN=%s",
                    count, wordLength, category, isTopN
                );
                
                result.set(analysis);
                context.write(key, result);
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit summary statistics
            Text summaryKey = new Text("### SUMMARY_STATISTICS ###");
            StringBuilder summary = new StringBuilder();
            
            summary.append(String.format("total_word_occurrences=%d, ", totalWords));
            summary.append(String.format("unique_words=%d, ", totalUniqueWords));
            
            if (totalUniqueWords > 0) {
                double avgFreq = (double) totalWords / totalUniqueWords;
                summary.append(String.format("avg_frequency=%.2f, ", avgFreq));
            }
            
            // Most common word length
            int mostCommonLength = 0;
            int maxCount = 0;
            for (Map.Entry<Integer, Integer> entry : lengthDistribution.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    mostCommonLength = entry.getKey();
                }
            }
            summary.append(String.format("most_common_length=%d", mostCommonLength));
            
            result.set(summary.toString());
            context.write(summaryKey, result);
        }
        
        private String categorizeWord(String word) {
            if (word.length() <= 3) {
                return "SHORT";
            } else if (word.length() <= 7) {
                return "MEDIUM";
            } else if (word.length() <= 12) {
                return "LONG";
            } else {
                return "VERY_LONG";
            }
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: FinalAnalysisJob <wordcount_input> <topn_input> <output>");
            return -1;
        }
        
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Final Filtered Ranking and Analysis");
        
        job.setJarByClass(FinalAnalysisJob.class);
        job.setReducerClass(FinalAnalysisReducer.class);
        
        // Multiple inputs from different jobs
        MultipleInputs.addInputPath(job, new Path(args[0]), 
            TextInputFormat.class, WordCountDataMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), 
            TextInputFormat.class, TopNDataMapper.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}