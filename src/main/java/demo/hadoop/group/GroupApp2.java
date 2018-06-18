package demo.hadoop.group;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * 排序，如果第一个数相同，则第二个数选择最小的
 * 
 * 思路2：第一个数为key，第二个数为value
 * 
 * @author qingjian
 *
 */
public class GroupApp2 {
	static final String INPUT_PATH = "hdfs://localhost:9000/eclipse/data";
	static final String OUTPUT_PATH = "hdfs://localhost:9000/out";
	static void check(FileSystem fileSystem) throws IOException {
		if(!fileSystem.exists(new Path(INPUT_PATH))) {
			System.err.println("Usage:Data Source not Found!");
			System.exit(1);
		}
		if(fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
	}
	 static void output(FileSystem fileSystem, Configuration conf) throws IOException {
		final String RESULT = "hdfs://localhost:9000/out/part-r-00000"; 
		FSDataInputStream in = fileSystem.open(new Path(RESULT));
		IOUtils.copyBytes(in, System.out, conf, true);
	}
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		
		check(fileSystem);
		
		Job job = new Job(conf, GroupApp2.class.getSimpleName()); 
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		
		job.waitForCompletion(true);
		
		output(fileSystem, conf);
		
	}
	static class MyMap extends Mapper<LongWritable, Text, Text, LongWritable> {
		Text keyInfo = new Text();
		LongWritable valueInfo = new LongWritable();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			keyInfo.set(split[0]);
			valueInfo.set(Long.parseLong(split[1]));
			context.write(keyInfo, valueInfo);
		}
	}
	static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long min = values.iterator().next().get();
			for (LongWritable val: values) {
				if(min > val.get()) {
					min = val.get();
				}
			}
			context.write(key, new LongWritable(min));
		}
	}
	
}
