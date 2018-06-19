package demo.hadoop.oldapi;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
/**
 * hadoop版本1.x的包一般是mapreduce
 * hadoop版本0.x的包一般是mapred
 *
 */
public class OldMapReduce {
	static final String INPUT_PATH="hdfs://localhost:9000/eclipse/hello";
	static final String OUTPUT_PATH="hdfs://localhost:9000/out";
	/**
	 * 改动：
	 * 1.不再使用Job，而是使用JobConf
	 * 2.类的包名不再使用mapreduce，而是使用mapred(尤其注意的是FileInputFormat、FileOutputFormat引用的是mapred包)
	 * 3.不再使用job.waitForCompletion(true)提交作业，而是使用JobClient.runJob(job);
	 * 
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		//检查路径
		check(fileSystem);
		JobConf job = new JobConf(conf, OldMapReduce.class);
		//设置map/reduce类
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		//设置输出
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//设置输入输出路径
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		
		//把job提交给JobTracker运行
		//job.waitForCompletion(true)
		JobClient.runJob(job);
		//打印输出结果
		output(fileSystem, conf);
		
	}
	
	static void check(FileSystem fileSystem) throws IOException {
		if(!fileSystem.exists(new Path(INPUT_PATH))) {
			System.err.println("Usage: Data Source not Found");
			System.exit(1);
		}
		if(fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
	}
	static void output(FileSystem fileSystem, Configuration conf) throws IOException {
		//final String RESULT = "hdfs://localhost:9000/out/part-r-00000";//新输出文件名
		final String RESULT = "hdfs://localhost:9000/out/part-00000"; //老输出文件名
		FSDataInputStream in = fileSystem.open(new Path(RESULT));
		IOUtils.copyBytes(in, System.out, conf);
	}
	static class MyMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			String[] split = value.toString().split("\t");
			for (String word : split) {
				output.collect(new Text(word), new LongWritable(1));
			}
			
		}
		
	}
	
	static class MyReduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			long sum=0L;
			while(values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new LongWritable(sum));
			
		}
		
	}
}
