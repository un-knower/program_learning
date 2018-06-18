package demo.hadoop.count;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCountApp {
	static final String INPUT_PATH = "hdfs://sparkmaster:9000/eclipse/hello";
	static final String OUT_PATH = "hdfs://sparkmaster:9000/out";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(new URI(OUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH))) {
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		Job job = new Job(conf, WordCountApp.class.getSimpleName());
		
		//1.1 输入目录在哪里
		FileInputFormat.setInputPaths(job, INPUT_PATH); //文件输入格式
		//指定对输入数据进行格式化处理的类（可省略）
		//job.setInputFormatClass(TextInputFormat.class); //文本文件内容
		
		//1.2指定自定义的Mapper类
		job.setMapperClass(MyMapper.class);
		//指定map输出的<k,v>类型（如果<k3,v3>与<k2,v2>一致，那么可省略）
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(LongWritable.class);
		
		//1.3 分区（可省略）
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setNumReduceTasks(1);
		
		//1.4 排序、分组
		
		//1.5 TODO（可选）归约
		
		//2.2 指定自定义的Reducer类
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//2.3 指定输出的路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		//指定输出的格式化类（可省略）
		//job.setOutputFormatClass(TextOutputFormat.class);
		//把作业交给JobTracher运行
		job.waitForCompletion(true);
	}
	/**
	 * KEYIN	即k1	表示一行的起始位置（偏移量offset）
	 * VALUEIN	即v1 表示每一行的文本内容
	 * KEYOUT	即k2	表示每一行中的每一个单词
	 * VALUEOUT	即v2	表示每一行中的每个单词的出现次数，固定值1
	 * @author qingjian
	 *
	 */
	static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			/**
			 * 计数器
			 */
			Counter counter = context.getCounter("Sensitive Word", "hello");
			if(v1.toString().contains("hello")) {//记录敏感词出现在一行中，假定敏感为Hello
				counter.increment(1L);
			}
			String[] splited = v1.toString().split("\t");
			for(String word:splited) {
				context.write(new Text(word), new LongWritable(1));
			}
			//super.map(k1, v1, context);
		}
		
	}
	
	/**
	 * KEYIN	即k2	表示每一行中的每一个单词
	 * VALUEIN	即v2	表示每一行中的每个单词的出现次数，固定值1
	 * KEYOUT	即k3	表示整个文件中的不同单词
	 * VALUEOUT	即v3	表示更个文件中的不同单词的出现总次数
	 * @author qingjian
	 *
	 */
	static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0L;
			for (LongWritable v2 : v2s) {
				sum += v2.get();
			}
			context.write(k2, new LongWritable(sum));
			//super.reduce(k2, v2s, context);
		}
		
	}
}
