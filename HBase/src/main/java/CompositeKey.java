import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CompositeKey implements WritableComparable<CompositeKey> {
    private Text host_;
    private BooleanWritable hasRobots_;

    public CompositeKey() {
        set(new Text(), new BooleanWritable());
    }

    public CompositeKey(String host, Boolean hasRobots) {
        set(new Text(host), new BooleanWritable(hasRobots));
    }

    private void set(Text host, BooleanWritable hasRobots) {
        host_ = host;
        hasRobots_ = hasRobots;
    }

    public String getHost() {
        return host_.toString();
    }

    public Boolean getHasRobots() {
        return hasRobots_.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        host_.write(out);
        hasRobots_.write(out);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        host_.readFields(dataInput);
        hasRobots_.readFields(dataInput);
    }

    @Override
    public int compareTo(@Nonnull CompositeKey o) {
        int cmp = host_.compareTo(o.host_);
        return (cmp == 0) ? -hasRobots_.compareTo(o.hasRobots_) : cmp;
    }
}