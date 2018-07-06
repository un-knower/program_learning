package demo.hadoop.frequence;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/**
 *
 * 计算频率
 * 输入
 *

 a,b,c,d
 b,a
 a
 c,b,d

 输出：
 [a, b, c]       1
 [a, b, d]       1
 [a, c, d]       1
 [b, c, d]       2

 */
class MBA_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static final int DEFAULT_NUMBER_OF_PAIRS = 2;
    //output key2: list of items paired; can be 2 or 3 ...
    private static final Text mapperKey = new Text();

    //output value2: number of the paired items in the item list
    private static final IntWritable NUMBER_ONE = new IntWritable(1);

    int numberOfPairs; // will be read by setup(), set by driver

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numberOfPairs = context.getConfiguration().getInt("number.of.pairs", DEFAULT_NUMBER_OF_PAIRS);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ",");
        if (tokens == null || tokens.length == 0) {
            return;
        }

        // generate mapper output
        List<String> items = new ArrayList<>();
        for (String token:tokens) {
            items.add(token);
        }
        List<List<String>> sortedCombinations = Combination.findSortedCombinations(items, numberOfPairs);
        for (List<String> itemList : sortedCombinations) {
            String itemString = itemList.toString();
            System.out.println(itemString);
            mapperKey.set(itemString);
            context.write(mapperKey, NUMBER_ONE);
        }
    }
}

class MBA_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

/**
 *
 * 计算频率
 * 输入
 *

 a,b,c,d
 b,a
 a
 c,b,d

 输出：
 [a, b, c]       1
 [a, b, d]       1
 [a, c, d]       1
 [b, c, d]       2

 */
public class MBA_MR {
    private static final String INPUT_PATH = "hdfs://SparkMaster:9000/eclipse/data/mba";
    private static final String OUTPUT_PATH = "hdfs://SparkMaster:9000/out";
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("number.of.pairs", 3);
        Job job = new Job(conf);
        job.setJarByClass(MBA_MR.class);
        job.setMapperClass(MBA_Mapper.class);
        job.setReducerClass(MBA_Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.waitForCompletion(true);

    }
}
