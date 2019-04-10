import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class PageRankNode implements Writable {
    private Text url_;
    private FloatWritable pageRank_;
    private LongWritable docId_;
    private TextArrayWritable links_;
    private BooleanWritable isLeaf_;
    private BooleanWritable realNode_;

    private String separator = " <<sep>> ";
    private String urlSeparator = " <<urlSep>> ";

    public static void main(String[] args) {
        String s = "url:=http://ka-news.de/region/karlsruhe/karlsruhe~/Geiselnahme-in-Karlsruhe-Taeter-schwer-bewaffnet-Verletzte;art6066,913326\tpageRank:=1.0\tdocId:=0\tisLeaf:=true\trealNode:=true\tlinksCount:=0\tlinks:=";
        PageRankNode node = new PageRankNode();
        node.fromString(s);
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class, new Text[]{});
        }

        public TextArrayWritable(Text[] values) {
            super(Text.class, values);
        }
    }

    public PageRankNode() {
        set(new Text(), new FloatWritable(), new LongWritable(), new TextArrayWritable(), new BooleanWritable(), new BooleanWritable());
    }

    public PageRankNode(String url, Float pageRank, Long docId, String[] links, Boolean isLeaf, Boolean realNode) {
        Text[] textLinks = new Text[links.length];
        for (int i = 0; i < links.length; i++) {
            textLinks[i] = new Text(links[i]);
        }
        set(new Text(url), new FloatWritable(pageRank), new LongWritable(docId), new TextArrayWritable(textLinks), new BooleanWritable(isLeaf), new BooleanWritable(realNode));
    }

    public PageRankNode(String url, Float pageRank, Boolean isLeaf, Boolean realNode) {
        set(new Text(url), new FloatWritable(pageRank), new LongWritable(), new TextArrayWritable(), new BooleanWritable(isLeaf), new BooleanWritable(realNode));
    }

    public Text getUrl() {
        return url_;
    }

    public String[] getLinks() {
        return links_.toStrings();
    }

    public Integer linksCount() {
        return links_.toStrings().length;
    }

    public Float getPageRank() {
        return pageRank_.get();
    }

    public Boolean realNode() {
        return realNode_.get();
    }

    public void setPageRank(Float newPageRank) {
        pageRank_ = new FloatWritable(newPageRank);
    }

    private void set(Text url, FloatWritable pageRank, LongWritable docId, TextArrayWritable links, BooleanWritable isLeaf, BooleanWritable realNode) {
        url_ = url;
        pageRank_ = pageRank;
        docId_ = docId;
        links_ = links;
        isLeaf_ = isLeaf;
        realNode_ = realNode;
    }

    public boolean isLeaf() {
        return isLeaf_.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        url_.write(out);
        pageRank_.write(out);
        docId_.write(out);
        links_.write(out);
        isLeaf_.write(out);
        realNode_.write(out);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        url_.readFields(input);
        pageRank_.readFields(input);
        docId_.readFields(input);
        links_.readFields(input);
        isLeaf_.readFields(input);
        realNode_.readFields(input);
    }

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        string.append("url:=").append(url_.toString()).append(separator).append("pageRank:=").append(pageRank_.toString()).append(separator)
                .append("docId:=").append(docId_.toString()).append(separator).append("isLeaf:=").append(isLeaf_.toString()).append(separator)
                .append("realNode:=").append(realNode_.toString());

        string.append(separator).append("linksCount:=").append(linksCount()).append(separator).append("links:=");
        for (String link: links_.toStrings()) {
            string.append(link).append(urlSeparator);
        }
        return string.toString();
    }

    public void fromString(String s) {
        String[] parts = s.split(separator);
        url_ = new Text(parts[0].split(":=")[1]);
        pageRank_ = new FloatWritable(Float.parseFloat(parts[1].split(":=")[1]));
        docId_ = new LongWritable(Long.parseLong(parts[2].split(":=")[1]));
        isLeaf_ = new BooleanWritable(Boolean.parseBoolean(parts[3].split(":=")[1]));
        realNode_ = new BooleanWritable(Boolean.parseBoolean(parts[4].split(":=")[1]));

        int linksCount = Integer.parseInt(parts[5].split(":=")[1]);
        if (linksCount != 0) {
            Text[] linksText = new Text[linksCount];
            String[] links = parts[6].split(":=")[1].split(urlSeparator);
            for (int i = 0; i < linksCount; i++) {
                linksText[i] = new Text(links[i]);
            }
            links_ = new TextArrayWritable(linksText);
        }
    }

    public static boolean isPageRankNodeString(String s) {
        return s.startsWith("url:=");
    }

}
