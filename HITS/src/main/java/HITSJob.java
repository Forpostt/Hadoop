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


public class HITSJob extends Configured implements Tool {
    private static String authorityPrefix = "authority:=";
    private static String hubPrefix = "hub:=";
    private static float normalizer = 0.1f;

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new HITSJob(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        String input = args[0], output = args[1] + 0;
        for (int i = 0; i < 5; i++) {
            Job job = getJobConf(getConf(), input, output);
            if (!job.waitForCompletion(true)) {
                return 1;
            }
            System.out.println("Iteration " + i + " finished");

            input = args[1] + i + "/part-*";
            output = args[1] + (i + 1);
        }
        return 0;
    }

    private Job getJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(CreateGraphJob.class);
        job.setJobName(CreateGraphJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            HITSNode node = HITSNode.fromString(value.toString());
            context.write(new Text(node.getUrl()), new Text(node.toString()));

            for (String link: node.getInLinks()) {
                context.write(new Text(link), new Text(hubPrefix + (node.getAuthority() * normalizer)));
            }

            for (String link: node.getOutLinks()) {
                context.write(new Text(link), new Text(authorityPrefix + (node.getHub() * normalizer)));
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            HITSNode keyNode = new HITSNode();

            String nodeString;
            float authority = 0.0f, hub = 0.0f;
            for (Text nodeText: value) {
                nodeString = nodeText.toString();

                if (HITSNode.isHITSNodeString(nodeString)) {
                    keyNode = HITSNode.fromString(nodeString);
                } else if (nodeString.startsWith(authorityPrefix)) {
                    authority += Float.parseFloat(nodeString.replace(authorityPrefix, ""));
                } else if (nodeString.startsWith(hubPrefix)) {
                    hub += Float.parseFloat(nodeString.replace(hubPrefix, ""));
                }
            }

            keyNode.setAuthority(authority);
            keyNode.setHub(hub);
            context.write(NullWritable.get(), new Text(keyNode.toString()));
        }
    }
}