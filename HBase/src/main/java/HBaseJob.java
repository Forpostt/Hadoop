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


public class HBaseJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new HBaseJob(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class HBaseGrouper extends WritableComparator {
        protected HBaseGrouper() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String a_first = ((CompositeKey)a).getHost();
            String b_first = ((CompositeKey)b).getHost();
            return a_first.compareTo(b_first);
        }
    }

    public static class HBasePartitioner extends Partitioner<CompositeKey, Text> {
        @Override
        public int getPartition(CompositeKey key, Text val, int numPartitions) {
            return Math.abs(key.getHost().hashCode()) % numPartitions;
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

    public static class HBaseMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String s = value.toString();
            if (s.startsWith("@")) {
                String[] parts = s.substring(1, s.length()).split("<>");
                String domain;

                try {
                    URI uri = new URI(parts[0]);
                    domain = uri.getHost();
                } catch (URISyntaxException e) {
                    System.out.println("Error");
                    return;
                }

                CompositeKey newKey = new CompositeKey(domain, false);
                CompositeValue newValue = new CompositeValue(parts[0], Boolean.parseBoolean(parts[1]));
                context.write(newKey, new Text(newValue.toString()));
            } else {
                String[] parts = s.split("<>");
                String domain;

                try {
                    URI uri = new URI(parts[0]);
                    domain = uri.getHost();
                } catch (URISyntaxException e) {
                    System.out.println("Error");
                    return;
                }

                CompositeKey newKey = new CompositeKey(domain, true);
                CompositeValue newValue = new CompositeValue(parts[1]);
                context.write(newKey, new Text(newValue.toString()));
            }
        }
    }

    public static class HBaseReducer extends Reducer<CompositeKey, Text, NullWritable, Text> {
        public static class RobotsWhatever {
            private String[] rules;

            public RobotsWhatever() {
                rules = new String[]{};
            }

            public RobotsWhatever(String robots) {
                rules = robots.split("@");
            }

            public Boolean isDisabled(String url) {
                boolean dis = false;

                for (String rule: rules) {
                    if (rule.startsWith("/")) {
                        dis = dis || url.startsWith(rule);
                    } else if (rule.startsWith("*")) {
                        dis = dis || url.contains(rule.substring(1, rule.length()));
                    } else if (rule.endsWith("$")) {
                        dis = dis || url.endsWith(rule.substring(rule.length() - 1));
                    }
                }
                return dis;
            }
        }

        @Override
        protected void reduce(CompositeKey key, Iterable<Text> value, Context context) throws IOException, InterruptedException  {
            RobotsWhatever robots = new RobotsWhatever();
            Boolean isDisabled;


            for (Text itemText: value) {
                CompositeValue item = new CompositeValue();
                item.fromString(itemText.toString());

                if (!item.getRobots().isEmpty()) {
                    robots = new RobotsWhatever(item.getRobots());
                    continue;
                }

                String path = "";
                try {
                    URI uri = new URI(item.getDocUrl());
                    path = uri.getPath();
                } catch (URISyntaxException e) {
                    System.out.println("Error");
                    return;
                }

                isDisabled = robots.isDisabled(path);

                if (isDisabled && !item.getDisabled()) {
                    context.write(NullWritable.get(), new Text(item.getDocUrl() + " " + true));
                } else if (!isDisabled && item.getDisabled()) {
                    context.write(NullWritable.get(), new Text(item.getDocUrl() + " " + false));
                }
            }
        }
    }

    private Job getJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseJob.class);
        job.setJobName(HBaseJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(HBaseMapper.class);
        job.setReducerClass(HBaseReducer.class);

        job.setPartitionerClass(HBasePartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(HBaseGrouper.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}