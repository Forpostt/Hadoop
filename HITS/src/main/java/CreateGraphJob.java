import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.net.URISyntaxException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.Inflater;
import java.util.zip.DataFormatException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class CreateGraphJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new CreateGraphJob(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(getConf(), args[0], args[1], args[2]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private Job getJobConf(Configuration conf, String input, String output, String urls) throws IOException {
        conf.set("urlsFile", urls);

        Job job = Job.getInstance(conf);
        job.setJarByClass(CreateGraphJob.class);
        job.setJobName(CreateGraphJob.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(ParserMapper.class);
        job.setReducerClass(ParserReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static class ParserMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static byte[] buffer = new byte[1024];
        private static HashMap<Long, String> docIdToUrl = new HashMap<>();

        private static String normalizeUrl(String url) {
            url = url.replace("https:", "http:");
            url = url.replace("www.", "");
            url = url.replace(" ", "");
            url = url.replace("<br/>", "");
            url = url.replace("<br>", "");
            url = url.replace("\"", "");

            if (url.startsWith("//")) {
                url = "http:" + url;
            }
            if (url.endsWith("/")) {
                url = url.substring(0, url.length() - 1);
            }

            try {
                URI uri = new URI(url).normalize();
                return uri.toString();
            } catch (URISyntaxException e) {
                System.out.println("Error: " + e.getMessage());
                return url;
            }
        }

        private static void loadUrlsFile (Configuration conf) throws IOException {
            String urlsFile = conf.get("urlsFile");
            Path urls = new Path(urlsFile);
            FileSystem fs = urls.getFileSystem(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(urls)));

            for (String line; (line = reader.readLine()) != null;) {
                String[] parts = line.split("\t");
                String normalizedUrl = normalizeUrl(parts[1]);

                docIdToUrl.put(Long.parseLong(parts[0]), normalizedUrl);
            }
            reader.close();
        }

        private static String decompressDoc(String compressed) throws IOException {
            byte[] decodedDoc = Base64.decodeBase64(compressed.getBytes());

            Inflater inflater = new Inflater();
            inflater.setInput(decodedDoc);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(decodedDoc.length);

            try {
                while (!inflater.finished()) {
                    int count = inflater.inflate(buffer);
                    outputStream.write(buffer, 0, count);
                }
            } catch (DataFormatException e) {
                System.out.println("Error in decompress: " + e.getMessage());
                return null;
            }

            outputStream.close();
            return outputStream.toString();
        }

        private static ArrayList<String> extractLinks(String html, String base_url) {
            ArrayList<String> result = new ArrayList<>();
            Document doc = Jsoup.parse(html, "http://" + base_url);

            Elements links = doc.select("a[href]");
            String linkAttr;

            for (Element link : links) {
                linkAttr = link.attr("abs:href");
                if (!linkAttr.isEmpty()) {
                    result.add(normalizeUrl(linkAttr));
                }
            }
            return result;
        }

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            loadUrlsFile(conf);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            Long docId = Long.parseLong(line[0]);

            String html = decompressDoc(line[1]);
            if (html == null) {
                return;
            }

            String docUrl = docIdToUrl.get(docId);
            URI base;
            try {
                base = new URI(docUrl);
            } catch (URISyntaxException exc) {
                System.out.println("Wrong base address format: " + docUrl);
                return;
            }

            ArrayList<String> links = extractLinks(html, base.getHost());
            String[] outLinks = new String[links.size()];

            int i = 0;
            for (String link: links) {
                context.write(new Text(link), new Text(docUrl));
                outLinks[i] = link;
                i++;
            }

            HITSNode node = new HITSNode(docUrl, 1.0f, 1.0f, docId, new String[]{}, outLinks);
            context.write(new Text(docUrl), new Text(node.toString()));
        }
    }

    public static class ParserReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            HITSNode keyNode = new HITSNode(key.toString(), 1.0f, 1.0f);
            ArrayList<String> inLinks = new ArrayList<>();

            for (Text outLink : value) {
                if (HITSNode.isHITSNodeString(outLink.toString())) {
                    keyNode = HITSNode.fromString(outLink.toString());
                }
                else {
                    inLinks.add(outLink.toString());
                }
            }

            keyNode.setInLinks(inLinks);
            context.write(NullWritable.get(), new Text(keyNode.toString()));
        }
    }
}
