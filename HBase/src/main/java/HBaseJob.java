import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


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

    public static class HBaseMapper extends TableMapper<CompositeKey, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException{
            TableSplit tableSplit = (TableSplit)context.getInputSplit();
            String tableName = new String(tableSplit.getTableName());

            if(tableName.equals(context.getConfiguration().get("webPagesPath"))) {
                byte[] url = columns.getValue(Bytes.toBytes("docs"), Bytes.toBytes("url"));
                byte[] isDisabled = columns.getValue(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));

                String urlString = Bytes.toString(url);

                String domain;
                try {
                    URI uri = new URI(urlString);
                    domain = uri.getHost();
                } catch (URISyntaxException e) {
                    System.out.println("Error: " + e.getMessage());
                    return;
                }
                context.write(new CompositeKey(domain, false), new Text(new CompositeValue(urlString, isDisabled != null).toString()));
            } else {
                byte[] site = columns.getValue(Bytes.toBytes("info"), Bytes.toBytes("site"));
                byte[] robots = columns.getValue(Bytes.toBytes("info"), Bytes.toBytes("robots"));

                String siteString = Bytes.toString(site);
                String robotsString = Bytes.toString(robots);

                String domain;
                try {
                    URI uri = new URI(siteString);
                    domain = uri.getHost();
                } catch (URISyntaxException e) {
                    System.out.println("Error: " + e.getMessage());
                    return;
                }
                context.write(new CompositeKey(domain, true), new Text(new CompositeValue(robotsString).toString()));
            }
        }
    }

    public static class HBaseReducer extends TableReducer<CompositeKey, Text, ImmutableBytesWritable> {
        public static class RobotsProcessing {
            private String[] rules;

            public RobotsProcessing() {
                rules = new String[]{};
            }

            public RobotsProcessing(String robots) {
                rules = robots.split("\n");
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

        private static byte[] getMD5Hash(String url) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] messageDigest = md.digest(url.getBytes());
                return DatatypeConverter.printHexBinary(messageDigest).toLowerCase().getBytes();

            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void reduce(CompositeKey key, Iterable<Text> value, Context context) throws IOException, InterruptedException  {
            RobotsProcessing robots = new RobotsProcessing();
            Boolean isDisabled;
            String path;
            CompositeValue item = new CompositeValue();

            for (Text itemText: value) {
                item.fromString(itemText.toString());

                if (!item.getRobots().isEmpty()) {
                    robots = new RobotsProcessing(item.getRobots());
                    continue;
                }

                try {
                    URI uri = new URI(item.getDocUrl());
                    path = uri.getPath();
                } catch (URISyntaxException e) {
                    System.out.println("Error: " + e.getMessage());
                    return;
                }

                isDisabled = robots.isDisabled(path);

                if (isDisabled && !item.getDisabled()) {
                    Put put = new Put(getMD5Hash(item.getDocUrl()));
                    put.add(Bytes.toBytes("docs"), Bytes.toBytes("disabled"), Bytes.toBytes("Y"));
                    context.write(null, put);
                } else if (!isDisabled && item.getDisabled()) {
                    Put put = new Put(getMD5Hash(item.getDocUrl()));
                    put.add(Bytes.toBytes("docs"), Bytes.toBytes("disabled"), Bytes.toBytes(""));
                    context.write(null, put);
                }
            }
        }
    }

    private Job getJobConf(Configuration conf, String webPagesPath, String webSitesPath) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseJob.class);
        job.setJobName(HBaseJob.class.getCanonicalName());

        List<Scan> inputTables = new ArrayList<>();

        job.getConfiguration().set("webPagesPath", webPagesPath);
        job.getConfiguration().set("webSitesPath", webSitesPath);

        inputTables.add(new Scan());
        inputTables.add(new Scan());
        inputTables.get(0).setAttribute("scan.attributes.table.name", Bytes.toBytes(webPagesPath));
        inputTables.get(1).setAttribute("scan.attributes.table.name", Bytes.toBytes(webSitesPath));

        TableMapReduceUtil.initTableMapperJob(
                inputTables,
                HBaseMapper.class,
                CompositeKey.class,
                Text.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                webPagesPath,
                HBaseReducer.class,
                job
        );

        job.setNumReduceTasks(2);

        job.setPartitionerClass(HBasePartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(HBaseGrouper.class);

        return job;
    }
}