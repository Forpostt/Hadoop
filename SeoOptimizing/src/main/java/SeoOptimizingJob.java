import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class SeoOptimizingJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new SeoOptimizingJob(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class SeoOptimizingMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
        private CompositeKey parse(Text text) {
            String[] line = text.toString().split("\t");
            String query = line[0];
            String host = "";

            try {
                URI url = new URI(line[1]);
                host = url.getHost();
            } catch (URISyntaxException e) {
                System.out.println("Error: " + e.getMessage());
            }

            return new CompositeKey(query, host);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            CompositeKey new_key = parse(value);
            context.write(new_key, new_key.getQuery());
        }
    }

    public static class SeoOptimizingReducer extends Reducer<CompositeKey, Text, Text, IntWritable> {
        @Override
        protected void reduce(CompositeKey key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int maxQueryCount = 0, queryCount = 0;
            String curQuery = "", bestQuery = "";

            for (Text query: value) {
                String queryString = query.toString();

                if (!queryString.equals(curQuery)) {
                    if (queryCount > maxQueryCount) {
                        maxQueryCount = queryCount;
                        bestQuery = curQuery;
                    }

                    queryCount = 1;
                    curQuery = queryString;
                } else {
                    queryCount++;
                }
            }

            if (queryCount > maxQueryCount) {
                maxQueryCount = queryCount;
                bestQuery = curQuery;
            }

            context.write(new Text(key.getUrl() + "\t" + bestQuery), new IntWritable(maxQueryCount));
        }
    }

    public static class SeoOptimizingGrouper extends WritableComparator {
        protected SeoOptimizingGrouper() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text a_first = ((CompositeKey)a).getUrl();
            Text b_first = ((CompositeKey)b).getUrl();
            return a_first.compareTo(b_first);
        }
    }

    public static class SeoOptimizingPartitioner extends Partitioner<CompositeKey, Text> {
        @Override
        public int getPartition(CompositeKey key, Text val, int numPartitions) {
            return Math.abs(key.getUrl().hashCode()) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((CompositeKey)a).compareTo((CompositeKey)b);
        }
    }

    private Job GetJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(SeoOptimizingJob.class);
        job.setJobName(SeoOptimizingJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(SeoOptimizingMapper.class);
        job.setReducerClass(SeoOptimizingReducer.class);

        job.setPartitionerClass(SeoOptimizingPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(SeoOptimizingGrouper.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.out.println(input);

        return job;
    }
}


