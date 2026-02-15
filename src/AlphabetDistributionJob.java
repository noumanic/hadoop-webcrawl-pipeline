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
 * Job 4: Alphabet Distribution Analysis
 * 
 * Purpose: Analyze the distribution of starting letters and character frequencies
 * Helps understand structural patterns in the web text corpus
 * 
 * Input: Cleaned text from Job 1
 * Output: Letter distribution statistics (letter -> count, percentage)
 */
public class AlphabetDistributionJob extends Configured implements Tool {
    
    /**
     * Mapper: Emit first letter of each word and count all letters
     */
    public static class AlphabetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private Text letter = new Text();
        private final static IntWritable one = new IntWritable(1);
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString().trim();
            
            if (line.isEmpty()) {
                return;
            }
            
            // Extract word
            String[] parts = line.split("\\s+");
            if (parts.length > 0 && !parts[0].isEmpty()) {
                String word = parts[0].toLowerCase();
                
                // Emit first letter
                if (word.length() > 0 && Character.isLetter(word.charAt(0))) {
                    letter.set("FIRST_" + word.charAt(0));
                    context.write(letter, one);
                }
                
                // Emit each letter in the word
                for (char c : word.toCharArray()) {
                    if (Character.isLetter(c)) {
                        letter.set("CHAR_" + c);
                        context.write(letter, one);
                    }
                }
            }
        }
    }
    
    /**
     * Combiner: Local aggregation
     */
    public static class AlphabetCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();
        
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
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
     * Reducer: Aggregate letter counts and compute statistics
     */
    public static class AlphabetReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();
        private long totalCount = 0;
        
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            result.set(sum);
            context.write(key, result);
            totalCount += sum;
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit total count for this reducer's partition (not global total)
            Text totalKey = new Text("TOTAL_CHARS");
            IntWritable totalValue = new IntWritable((int) totalCount);
            context.write(totalKey, totalValue);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Alphabet Distribution Analysis");
        
        job.setJarByClass(AlphabetDistributionJob.class);
        job.setMapperClass(AlphabetMapper.class);
        job.setCombinerClass(AlphabetCombiner.class);
        job.setReducerClass(AlphabetReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}