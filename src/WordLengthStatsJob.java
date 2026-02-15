import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Job 3: Word Length Statistics
 * 
 * Purpose: Analyze the distribution of word lengths in the dataset
 * Calculate statistics for each word length category
 * 
 * Input: Cleaned text from Job 1
 * Output: Word length statistics (length -> count, avg_occurrences)
 */
public class WordLengthStatsJob extends Configured implements Tool {
    
    /**
     * Mapper: Emit word length as key with value 1
     */
    public static class WordLengthMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        
        private IntWritable wordLength = new IntWritable();
        private final static IntWritable one = new IntWritable(1);
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString().trim();
            
            if (line.isEmpty()) {
                return;
            }
            
            // Extract word (first field)
            String[] parts = line.split("\\s+");
            if (parts.length > 0 && !parts[0].isEmpty()) {
                String word = parts[0];
                wordLength.set(word.length());
                context.write(wordLength, one);
            }
        }
    }
    
    /**
     * Combiner: Local aggregation of word length counts
     */
    public static class WordLengthCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        
        private IntWritable result = new IntWritable();
        
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    /**
     * Reducer: Aggregate and compute statistics for each word length
     */
    public static class WordLengthReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        
        private Text result = new Text();
        
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int totalCount = 0;
            int numValues = 0;
            
            for (IntWritable val : values) {
                totalCount += val.get();
                numValues++;
            }
            
            double average = numValues > 0 ? (double) totalCount / numValues : 0.0;
            
            // Format: "count=X, avg_per_partition=Y"
            String stats = String.format("count=%d, avg_per_partition=%.2f", 
                totalCount, average);
            result.set(stats);
            
            context.write(key, result);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Word Length Statistics");
        
        job.setJarByClass(WordLengthStatsJob.class);
        job.setMapperClass(WordLengthMapper.class);
        job.setCombinerClass(WordLengthCombiner.class);
        job.setReducerClass(WordLengthReducer.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}