import org.apache.commons.codec.binary.Base64;

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
import java.io.ByteArrayOutputStream;

import java.util.ArrayList;
import java.util.zip.Inflater;
import java.util.zip.DataFormatException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class Parser extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Parser(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private Job getJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(Parser.class);
        job.setJobName(Parser.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(ParserMapper.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class ParserMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private static String decompress(String compressed) throws IOException, DataFormatException {
            byte[] decodedDoc = Base64.decodeBase64(compressed.getBytes());

            Inflater inflater = new Inflater();
            inflater.setInput(decodedDoc);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(decodedDoc.length);
            byte[] buffer = new byte[1024];
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }

            outputStream.close();
            return outputStream.toString();
        }

        private static ArrayList<String> extractLinks(String html) throws IOException {
            ArrayList<String> result = new ArrayList<String>();
            Document doc = Jsoup.parse(html);

            Elements links = doc.select("a[href]");
            String linkAttr;
            for (Element link : links) {
                linkAttr = link.attr("abs:href");
                if (!linkAttr.isEmpty()) {
                    // Fixme: remove http from links
                    result.add(linkAttr);
                }
            }

            return result;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String html;

            try {
                html = decompress(line[1]);
            } catch (DataFormatException e) {
                System.out.println("Error: " + e.getMessage());
                return;
            }

            ArrayList<String> links = extractLinks(html);
            StringBuilder result = new StringBuilder();
            for (String link: links) {
                result.append("\t");
                result.append(link);
            }

            context.write(key, new Text(result.toString()));
        }
    }
}
