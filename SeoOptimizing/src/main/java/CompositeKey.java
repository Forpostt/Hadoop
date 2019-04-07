import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CompositeKey implements WritableComparable<CompositeKey> {
    private Text query_;
    private Text url_;

    public CompositeKey() {
        set(new Text(), new Text());
    }

    public CompositeKey(Text query, Text url) {
        set(query, url);
    }

    public CompositeKey(String query, String url) {
        set(new Text(query), new Text(url));
    }

    private void set(Text query, Text url) {
        query_ = query;
        url_ = url;
    }

    public Text getQuery() {
        return query_;
    }

    public Text getUrl() {
        return url_;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        query_.write(out);
        url_.write(out);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        query_.readFields(dataInput);
        url_.readFields(dataInput);
    }

    @Override
    public int compareTo(@Nonnull CompositeKey o) {
        int cmp = url_.compareTo(o.url_);
        return (cmp == 0) ? -query_.compareTo(o.query_) : cmp;
    }
}