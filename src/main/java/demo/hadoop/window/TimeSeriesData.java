package demo.hadoop.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用作key，借助hadoop shuffle机制需要排序
 */
public class TimeSeriesData implements WritableComparable<TimeSeriesData> {
    private String company = "";
    private long timestamp = 0L;

    public TimeSeriesData() {}
    public TimeSeriesData(String company, long timestamp) {
        this.company = company;
        this.timestamp = timestamp;
    }
    @Override
    public int compareTo(TimeSeriesData o) {
        if (StringUtils.equals(company, o.company)) { // 如果公司一样，则按照时间排序
            return Long.compare(timestamp, o.timestamp);
        } else {
            return StringUtils.compare(company, o.company);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(company);
        out.writeLong(timestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        company = in.readUTF();
        timestamp = in.readLong();
    }


//    @Override
//    public int hashCode() {
//        return Objects.hash(company);  // 按照company进行分组,需要配合WritableComparator，job.setGroupingComparatorClass(DataGroupComparator.class);
//    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "TimeSeriesData{" +
                "company='" + company + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
