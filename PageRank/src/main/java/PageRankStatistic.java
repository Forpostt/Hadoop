import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;


public class PageRankStatistic extends Configured implements Tool {
    public static int topN = 30;
    public static Text totalNodesKey = new Text("totalNodes");
    public static Text totalLeafsKey = new Text("totalLeafs");
    public static Text nodeKey = new Text("nodeKey");

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new PageRankStatistic(), args);
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
        job.setReducerClass(StatisticReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class StatisticMapper extends Mapper<LongWritable, Text, Text, Text> {
        TreeSet<PageRankNode> topAuthority = new TreeSet<>();
        private long totalNodes = 0;
        private long totalLeafs = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            PageRankNode node = PageRankNode.fromString(value.toString());
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
            for (PageRankNode node: topAuthority) {
                node.cleanLinks();
                context.write(nodeKey, new Text(node.toString()));
            }
            context.write(totalNodesKey, new Text(Long.toString(totalNodes)));
            context.write(totalLeafsKey, new Text(Long.toString(totalLeafs)));
        }
    }

    public static class StatisticReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            if (key.equals(nodeKey)) {
                TreeSet<PageRankNode> topAuthority = new TreeSet<>();
                for (Text nodeString: value) {
                    PageRankNode node = PageRankNode.fromString(nodeString.toString());
                    topAuthority.add(node);

                    if (topAuthority.size() > topN) {
                        topAuthority.pollFirst();
                    }
                }
                Iterator itr = topAuthority.descendingIterator();
                while (itr.hasNext()) {
                    PageRankNode node = (PageRankNode) itr.next();
                    node.cleanLinks();
                    context.write(NullWritable.get(), new Text(node.toString()));
                }
            } else if (key.equals(totalLeafsKey)) {
                long totalLeafs = 0L;
                for (Text str: value) {
                    totalLeafs += Long.parseLong(str.toString());
                }
                context.write(NullWritable.get(), new Text(totalLeafsKey.toString() + ":=" + totalLeafs));
            } else if (key.equals(totalNodesKey)) {
                long totalNodes = 0L;
                for (Text str: value) {
                    totalNodes += Long.parseLong(str.toString());
                }
                context.write(NullWritable.get(), new Text(totalNodesKey.toString() + ":=" + totalNodes));
            }
        }
    }

}
