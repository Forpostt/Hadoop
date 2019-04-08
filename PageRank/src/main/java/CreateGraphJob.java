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

import java.util.ArrayList;
import java.util.HashMap;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.HashSet;
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

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PageRankNode.class);

        return job;
    }

    public static class ParserMapper extends Mapper<LongWritable, Text, Text, PageRankNode> {
        private static byte[] buffer = new byte[1024];
        private static HashMap<Long, String> docIdToUrl = new HashMap<>();
        private static HashSet<String> docUrls = new HashSet<>();
        private static HashSet<String> processedUrls = new HashSet<>();

        private static String normalizeUrl(String url) {
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
                docUrls.add(normalizedUrl);
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
                System.out.println("Error: " + e.getMessage());
                return null;
            }

            outputStream.close();
            return outputStream.toString();
        }

        private static ArrayList<String> extractLinks(String html) {
            ArrayList<String> result = new ArrayList<>();
            Document doc = Jsoup.parse(html);

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

            ArrayList<String> links = extractLinks(html);
            String[] nodeLinks = new String[links.size()];

            int i = 0;
            for (String link: links) {
                // Create PageRankNode for LeafNode
                if (!docUrls.contains(link) && !processedUrls.contains(link)) {
                    context.write(new Text(link), new PageRankNode(link, 1.0f, true));
                    processedUrls.add(link);
                }

                nodeLinks[i] = link;
                i++;
            }

            String docUrl = docIdToUrl.get(docId);
            Boolean isLeaf = nodeLinks.length == 0;
            if (!processedUrls.contains(docUrl)) {
                PageRankNode node = new PageRankNode(docUrl, 1.0f, docId, nodeLinks, isLeaf);
                context.write(node.getUrl(), node);
                processedUrls.add(docUrl);
            }
        }
    }

    public static class ParserReducer extends Reducer<Text, PageRankNode, Text, PageRankNode> {
        @Override
        protected void reduce(Text key, Iterable<PageRankNode> value, Context context) throws IOException, InterruptedException {
            PageRankNode keyNode = new PageRankNode(key.toString(), 1.0f, true);

            for (PageRankNode node: value) {
                if (!node.isLeaf()) {
                    keyNode = node;
                    break;
                }
            }

            context.write(key, keyNode);
        }
    }
}
