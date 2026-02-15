import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Job 1: Text Cleaning and Normalization
 * 
 * Purpose: Clean raw WET file text by:
 * - Removing HTML entities and special characters
 * - Converting to lowercase
 * - Removing excessive whitespace
 * - Filtering out URLs and email addresses
 * - Keeping Unicode letters (\p{L}) for multi-language text (CJK, Arabic, etc.)
 * - Filtering tokens shorter than 2 characters
 * 
 * Input: Raw WET files (e.g. dataset-downloader/downloaded_wet_files/data-*)
 * Output: Cleaned, normalized text (one word per line); supports multi-language (CJK, etc.)
 */
public class TextCleaningJob extends Configured implements Tool {
    
    /**
     * Mapper: Clean and normalize text from WET files
     */
    public static class CleaningMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text cleanedWord = new Text();
        private Text emptyValue = new Text("");
        
        // Patterns for cleaning
        private static final Pattern URL_PATTERN = Pattern.compile(
            "https?://[^\\s]+|www\\.[^\\s]+");
        private static final Pattern EMAIL_PATTERN = Pattern.compile(
            "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}");
        private static final Pattern HTML_ENTITY_PATTERN = Pattern.compile(
            "&[a-zA-Z]+;|&#[0-9]+;");
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            
            // Skip empty lines
            if (line.trim().isEmpty()) {
                return;
            }
            
            // Skip WARC header lines (WET files: warcinfo, conversion records, metadata)
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                return;
            }
            String lower = trimmed.toLowerCase();
            if (trimmed.startsWith("WARC/") || trimmed.startsWith("WARC-")
                    || trimmed.startsWith("Content-") || trimmed.startsWith("Content-Length")
                    || trimmed.startsWith("Software-") || trimmed.startsWith("Extracted-")
                    || lower.startsWith("robots:") || lower.startsWith("ispartof:")
                    || lower.startsWith("operator:") || lower.startsWith("publisher:")
                    || lower.startsWith("description:")) {
                return;
            }
            // Skip other common WARC/HTTP header-style lines (Key: value at start)
            if (trimmed.matches("^[A-Za-z][A-Za-z0-9_-]*:\\s.*")) {
                int colon = trimmed.indexOf(':');
                String key = trimmed.substring(0, colon).toLowerCase();
                if (key.startsWith("warc") || key.equals("content-type") || key.equals("content-length")) {
                    return;
                }
            }
            
            // Remove URLs (http, https, www)
            line = URL_PATTERN.matcher(line).replaceAll("");
            
            // Remove email addresses
            line = EMAIL_PATTERN.matcher(line).replaceAll("");
            
            // Remove HTML entities
            line = HTML_ENTITY_PATTERN.matcher(line).replaceAll("");
            
            // Convert to lowercase (affects Latin script only; CJK/other unchanged)
            line = line.toLowerCase();
            
            // Remove non-alphabetic: keep Unicode letters (\p{L}) and spaces for multi-language support
            line = line.replaceAll("[^\\p{L}\\s]+", " ");
            
            // Split into words and emit
            String[] words = line.split("\\s+");
            
            for (String word : words) {
                // Remove leading/trailing whitespace
                word = word.trim();
                
                // Only emit words with 2 or more characters
                if (word.length() >= 2) {
                    cleanedWord.set(word);
                    context.write(cleanedWord, emptyValue);
                }
            }
        }
    }
    
    /**
     * Reducer: Remove duplicates and output unique cleaned words
     */
    public static class CleaningReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text emptyValue = new Text("");
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            // Simply output the word once (deduplication at this stage)
            context.write(key, emptyValue);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Text Cleaning and Normalization");
        
        job.setJarByClass(TextCleaningJob.class);
        job.setMapperClass(CleaningMapper.class);
        job.setReducerClass(CleaningReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}