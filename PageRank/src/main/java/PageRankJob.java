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

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;


public class PageRankJob extends Configured implements Tool {
    private static String leafsPageRankFile = "leafs_page_rank";
    private static String totalNodesFile = "total_nodes";

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new PageRankJob(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        String input = args[0], output = args[1] + 0;
        for (int i = 0; i < 7; i++) {
            FileWriter fw = new FileWriter(leafsPageRankFile);
            fw.close();
            fw = new FileWriter(totalNodesFile);
            fw.close();

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
        float leafsPageRank = 0.0f;
        long nodesCount = 0L;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Save node
            PageRankNode node = PageRankNode.fromString(value.toString());
            context.write(new Text(node.getUrl()), new Text(node.toString()));
            nodesCount++;

            if (node.isLeaf()) {
                leafsPageRank += node.getPageRank();
                return;
            }

            for (String link: node.getLinks()) {
                Float pageRank = node.getPageRank() / node.linksCount();
                context.write(new Text(link), new Text(pageRank.toString()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException {
            FileWriter fw = new FileWriter(leafsPageRankFile, true);
            fw.write(leafsPageRank + "\n");
            fw.close();

            fw = new FileWriter(totalNodesFile, true);
            fw.write(nodesCount + "\n");
            fw.close();
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, NullWritable, Text> {
        float leafsPageRank = 0.0f;
        long totalNodes = 0L;

        @Override
        protected void setup(Context context) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(leafsPageRankFile));
            for (String line; (line = reader.readLine()) != null; ) {
                leafsPageRank += Float.parseFloat(line);
            }

            reader = new BufferedReader(new FileReader(totalNodesFile));
            for (String line; (line = reader.readLine()) != null; ) {
                totalNodes += Long.parseLong(line);
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            PageRankNode keyNode = new PageRankNode(key.toString(), 0.0f, false);

            float pageRank = 0.0f;
            for (Text nodeString: value) {
                if (PageRankNode.isPageRankNodeString(nodeString.toString())) {
                    keyNode = PageRankNode.fromString(nodeString.toString());
                } else {
                    pageRank += Float.parseFloat(nodeString.toString());
                }
            }

            // page rank formula, when init page rank is 1.0
            pageRank = 0.85f * (pageRank + leafsPageRank / totalNodes) + 0.15f;

            keyNode.setPageRank(pageRank);
            context.write(NullWritable.get(), new Text(keyNode.toString()));
        }
    }
}