package demo.hadoop.suanfa;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * hadoop中的MapReduce框架里已经预定义了相关的接口，其中如Mapper类下的方法setup()和cleanup()。

    setup()，此方法被MapReduce框架仅且执行一次，在执行Map任务前，进行相关变量或者资源的集中初始化工作。
    若是将资源初始化工作放在方法map()中，导致Mapper任务在解析每一行输入时都会进行资源初始化工作，导致重复，程序运行效率不高！
    cleanup(),此方法被MapReduce框架仅且执行一次，在执行完毕Map任务后，进行相关变量或资源的释放工作。
    若是将释放资源工作放入方法map()中，也会导致Mapper任务在解析、处理每一行文本后释放资源，而且在下一行文本解析前还要重复初始化，导致反复重复，程序运行效率不高！

	所以，建议资源初始化及释放工作，分别放入方法setup()和cleanup()中进行。
 *
 *
 *
 * 每个map任务取到最大值，传递给reduce，再在reduce中取得最大值
 */
public class TopKApp {
	static final Path INPUT_PATH = new Path("hdfs://localhost:9000/eclipse/seq100w.txt");
	static final Path OUTPUT_PATH = new Path("hdfs://localhost:9000/out");
	static final Path RESULT_PATH = new Path("hdfs://localhost:9000/out/part-r-00000");
	public static void check(FileSystem fileSystem) throws IOException {
		if(!fileSystem.exists(INPUT_PATH)) {
			System.err.println("Usage:Data Source not Found!");
			System.exit(1);
		}
		if(fileSystem.exists(OUTPUT_PATH)) {
			fileSystem.delete(OUTPUT_PATH, true);
		}
	}
	public static void output(Configuration conf, FileSystem fileSystem) throws IOException {
		FSDataInputStream in = fileSystem.open(RESULT_PATH);
		IOUtils.copyBytes(in, System.out, conf);
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(INPUT_PATH.toUri(), conf);
		
		check(fileSystem);
		
		Job job = new Job(conf,TopKApp.class.getSimpleName());
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, OUTPUT_PATH);
		if(job.waitForCompletion(true)) {
			output(conf, fileSystem);
		}
	}
	static class MyMap extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
		long max = Long.MIN_VALUE;
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			long temp = Long.parseLong(value.toString()); 
			if(max< temp) {
				max = temp;
			}
		}
		
		//在所有map任务执行完毕后
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}
	}
	static class MyReduce extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
		long max = Long.MIN_VALUE;//-9223372036854775808
		//所有reduce任务执行完毕之后，执行cleanup()方法
		@Override
		protected void cleanup(
				Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}

		@Override
		protected void reduce(
				LongWritable key,
				Iterable<NullWritable> values,
				Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			long temp = key.get();
			if(max< temp)
			{
				max = temp;
			}
		}
		
	}
}