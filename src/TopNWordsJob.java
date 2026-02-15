import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Job 5: Top-N Frequent Words Identification
 * 
 * Purpose: Identify the most frequent words in the dataset
 * Uses secondary sorting to rank words by frequency
 * 
 * Input: Word counts from Job 2
 * Output: Top N words with their frequencies (sorted by count descending)
 */
public class TopNWordsJob extends Configured implements Tool {
    
    private static final int TOP_N = 1000; // Top 1000 words
    
    /**
     * Mapper: Swap key-value pairs (word, count) -> (count, word)
     * This allows sorting by count
     */
    public static class TopNMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        private IntWritable count = new IntWritable();
        private Text word = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString().trim();
            
            if (line.isEmpty()) {
                return;
            }
            
            // Parse "word\tcount" format (tab-separated for robustness)
            String[] parts = line.split("\\t", 2);
            if (parts.length >= 2) {
                try {
                    String wordStr = parts[0];
                    int countValue = Integer.parseInt(parts[1]);
                    
                    // Emit swapped key-value
                    count.set(countValue);
                    word.set(wordStr);
                    context.write(count, word);
                } catch (NumberFormatException e) {
                    // Skip malformed lines
                }
            }
        }
    }
    
    /**
     * Custom comparator for descending order
     */
    public static class DescendingIntComparator extends WritableComparator {
        
        protected DescendingIntComparator() {
            super(IntWritable.class, true);
        }
        
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntWritable key1 = (IntWritable) a;
            IntWritable key2 = (IntWritable) b;
            // Reverse order for descending sort
            return -1 * key1.compareTo(key2);
        }
    }
    
    /**
     * Reducer: Keep only top N words
     */
    public static class TopNReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        
        private TreeMap<Integer, String> topWords = new TreeMap<>();
        private int rank = 0;
        
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            // Skip if we already have enough (keys arrive in descending count order)
            if (rank >= TOP_N) {
                return;
            }
            // Process all words with this count
            for (Text word : values) {
                if (rank < TOP_N) {
                    topWords.put(rank++, word.toString() + "\t" + key.get());
                } else {
                    break;
                }
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit top N words
            for (int i = 0; i < Math.min(TOP_N, topWords.size()); i++) {
                String entry = topWords.get(i);
                if (entry != null) {
                    String[] parts = entry.split("\\t");
                    if (parts.length == 2) {
                        Text word = new Text(parts[0]);
                        IntWritable count = new IntWritable(Integer.parseInt(parts[1]));
                        context.write(word, count);
                    }
                }
            }
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Top-N Frequent Words");
        
        job.setJarByClass(TopNWordsJob.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        
        // Use single reducer to get global top N
        job.setNumReduceTasks(1);
        
        // Set custom comparator for descending order
        job.setSortComparatorClass(DescendingIntComparator.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}