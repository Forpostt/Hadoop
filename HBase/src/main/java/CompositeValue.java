import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CompositeValue {
    private Text docUrl_;
    private BooleanWritable disabled_;
    private Text robots_;

    private String separator = "<<sep>>";

    public CompositeValue() {
        set(new Text(), new BooleanWritable(), new Text());
    }

    public CompositeValue(String docUrl, Boolean disabled) {
        set(new Text(docUrl), new BooleanWritable(disabled), new Text());
    }

    public CompositeValue(String robots) {
        set(new Text(), new BooleanWritable(), new Text(robots));
    }

    private void set(Text docUrl, BooleanWritable disabled, Text robots) {
        docUrl_ = docUrl;
        disabled_ = disabled;
        robots_ = robots;
    }

    public String getDocUrl() {
        return docUrl_.toString();
    }

    public Boolean getDisabled() {
        return disabled_.get();
    }

    public String getRobots() {
        return robots_.toString();
    }

    @Override
    public String toString() {
        return docUrl_.toString() + separator + disabled_.toString() + separator + robots_.toString();
    }

    public void fromString(String s) {
        String[] parts = s.split(separator);
        docUrl_ = new Text(parts[0]);
        disabled_ = new BooleanWritable(Boolean.parseBoolean(parts[1]));
        if (parts.length > 2) {
            robots_ = new Text(parts[2]);
        }
    }
}