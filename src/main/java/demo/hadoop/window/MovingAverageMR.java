package demo.hadoop.window;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * hadoop计算滑动窗口中的平均值
 */


/**
 * 决定那个key 划分到哪个reducer
 */
class DataGroupComparator extends WritableComparator {
    // 构造方法必须要有
    public DataGroupComparator() {
        super(TimeSeriesData.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TimeSeriesData t1 = (TimeSeriesData) a;
        TimeSeriesData t2 = (TimeSeriesData) b;
        return StringUtils.compare(t1.getCompany(), t2.getCompany());
    }
}
class MovingAverageMapper extends Mapper<LongWritable, Text, TimeSeriesData, TimeValueData> {
    TimeSeriesData mapperKey = new TimeSeriesData();
    TimeValueData mapperValue = new TimeValueData();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null) {
            return;
        }
        String data = value.toString();
        String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(data, ",");
        if (tokens.length == 3) {
            mapperKey.setCompany(tokens[0]);
            mapperKey.setTimestamp(TimeUtil.timeFormat2TimeStamp(tokens[1], "yyyy-MM-dd"));
            mapperValue.setTimestamp(TimeUtil.timeFormat2TimeStamp(tokens[1], "yyyy-MM-dd"));
            mapperValue.setValue(Double.parseDouble(tokens[2]));
            context.write(mapperKey, mapperValue);
        }
    }
}

class MovingAverageReducer extends Reducer<TimeSeriesData, TimeValueData, Text, NullWritable> {
    StringBuffer stringBuffer = new StringBuffer();
    Text result = new Text();
    private int windowSize = 4;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        windowSize = context.getConfiguration().getInt("moving.window.size", 3);

    }

    @Override
    protected void reduce(TimeSeriesData key, Iterable<TimeValueData> values, Context context) throws IOException, InterruptedException {
        SimpleMovingAverage simpleMovingAverage = new SimpleMovingAverage(windowSize);

        stringBuffer.setLength(0);
        stringBuffer.append(key.getCompany()).append("\t");
        System.out.print(key.getCompany()+"\t");
        for (TimeValueData value : values) {
            simpleMovingAverage.addNeNumber(value.getValue()); // 增加一个数字
            stringBuffer.append(value.getTimestamp()+":"+simpleMovingAverage.getMovingAverage()+"\t");

            System.out.print(value.getTimestamp()+":"+simpleMovingAverage.getMovingAverage()+"\t");
        }
        System.out.println();
        result.set(stringBuffer.toString());
        context.write(result, NullWritable.get());
    }
}

/**
 * hadoop计算滑动窗口中的平均值
 */
public class MovingAverageMR {
    private static final String INPUT_PATH = "hdfs://SparkMaster:9000/eclipse/data/window_data";
    private static final String OUTPUT_PATH = "hdfs://SparkMaster:9000/out";
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("moving.window.size", 4);

        Job job = new Job(conf, MovingAverageMR.class.getName());
        job.setJarByClass(MovingAverageMR.class);
        job.setGroupingComparatorClass(DataGroupComparator.class);  // 决定那个key 划分到哪个reducer
        job.setMapperClass(MovingAverageMapper.class);
        job.setReducerClass(MovingAverageReducer.class);
        job.setMapOutputKeyClass(TimeSeriesData.class);
        job.setMapOutputValueClass(TimeValueData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, INPUT_PATH);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.waitForCompletion(true);

    }

}
