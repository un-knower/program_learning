package demo.hadoop.mapreduce_examples;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Uniq {
	//map将输入的value复制到输出数据key上，并直接输出
	public static class Map extends Mapper<Object, Text, Text, Text> {
		private static Text line = new Text();//每行数据
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			line = value;
			context.write(line, new Text(""));
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
		
	}
	//检测输入/输出路径
	public static void check(FileSystem fileSystem) throws IOException {
		if (!fileSystem.exists(new Path(INPUT_PATH))) {
			System.err.println("Usage: DAta Deduplication<in><out>");
			System.exit(2); //非正常关闭
		}
		if(fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
	}
	//显示输出结果
	public static void output(FileSystem fileSystem,Configuration conf) throws IOException {
		final String RESULT = "hdfs://localhost:9000/out/part-r-00000";
		FSDataInputStream in = fileSystem.open(new Path(RESULT));
		IOUtils.copyBytes(in, System.out, conf);
	}
	
	private static final String INPUT_PATH="hdfs://localhost:9000/eclipse/uniq";
	private static final String OUTPUT_PATH="hdfs://localhost:9000/out";
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		check(fileSystem);
		//设置job
		Job job = new Job(conf,Uniq.class.getSimpleName());
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		
		
		//
		job.waitForCompletion(true);
		
		//控制台打印输出结果
		output(fileSystem,conf);
	}
	
}
