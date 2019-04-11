import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.TreeSet;


public class HITSStatistic extends Configured implements Tool {
    public static int topN = 30;

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new HITSStatistic(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private Job getJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(CreateGraphJob.class);
        job.setJobName(CreateGraphJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(StatisticMapper.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class StatisticMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        TreeSet<HITSNode> topAuthority = new TreeSet<>();
        private long totalNodes = 0;
        private long totalLeafs = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            HITSNode node = HITSNode.fromString(value.toString());
            topAuthority.add(node);
            totalNodes++;

            if (topAuthority.size() > topN) {
                topAuthority.pollFirst();
            }

            if (node.isLeaf()) {
                totalLeafs++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            for (HITSNode node: topAuthority) {
                context.write(NullWritable.get(), new Text(node.toString()));
            }
            context.write(NullWritable.get(), new Text("totalNode:=" + totalNodes));
            context.write(NullWritable.get(), new Text("totalLeafs:=" + totalLeafs));
        }
    }

}
