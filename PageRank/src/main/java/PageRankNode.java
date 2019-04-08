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

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class, new Text[]{});
        }

        public TextArrayWritable(Text[] values) {
            super(Text.class, values);
        }
    }

    public PageRankNode() {
        set(new Text(), new FloatWritable(), new LongWritable(), new TextArrayWritable(), new BooleanWritable());
    }

    public PageRankNode(String url, Float pageRank, Long docId, String[] links, Boolean isLeaf) {
        Text[] textLinks = new Text[links.length];
        for (int i = 0; i < links.length; i++) {
            textLinks[i] = new Text(links[i]);
        }
        set(new Text(url), new FloatWritable(pageRank), new LongWritable(docId), new TextArrayWritable(textLinks), new BooleanWritable(isLeaf));
    }

    public PageRankNode(String url, Float pageRank, Boolean isLeaf) {
        set(new Text(url), new FloatWritable(pageRank), new LongWritable(), new TextArrayWritable(), new BooleanWritable(isLeaf));
    }

    public Text getUrl() {
        return url_;
    }

    private void set(Text url, FloatWritable pageRank, LongWritable docId, TextArrayWritable links, BooleanWritable isLeaf) {
        url_ = url;
        pageRank_ = pageRank;
        docId_ = docId;
        links_ = links;
        isLeaf_ = isLeaf;
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
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        url_.readFields(input);
        pageRank_.readFields(input);
        docId_.readFields(input);
        links_.readFields(input);
        isLeaf_.readFields(input);
    }

}
