package demo.hadoop.window;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用作value
 */
public class TimeValueData implements Writable {
    private long timestamp;
    private double value;

    public TimeValueData() {}
    public TimeValueData(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        timestamp = in.readLong();
        value = in.readDouble();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
