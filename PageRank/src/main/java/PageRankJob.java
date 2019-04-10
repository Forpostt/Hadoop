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


public class PageRankJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new PageRankJob(), args);
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
            // Save node
            PageRankNode node = new PageRankNode();
            node.fromString(value.toString());
            context.write(node.getUrl(), new Text(node.toString()));

            for (String link: node.getLinks()) {
                Float pageRank = node.getPageRank() / node.linksCount();
                context.write(new Text(link), new Text(pageRank.toString()));
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            PageRankNode keyNode = new PageRankNode(key.toString(), 0.0f, false, false);
            NullWritable nullWritable = NullWritable.get();

            Float pageRank = 0.0f;
            for (Text nodeString: value) {
                if (PageRankNode.isPageRankNodeString(nodeString.toString())) {
                    PageRankNode node = new PageRankNode();
                    node.fromString(nodeString.toString());
                    keyNode = node;

                } else {
                    pageRank += Float.parseFloat(nodeString.toString());
                }
            }
            pageRank = 0.15f * pageRank + 0.85f;

            keyNode.setPageRank(pageRank);
            context.write(nullWritable, new Text(keyNode.toString()));
        }
    }
}