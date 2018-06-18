package demo.hadoop.mapreduce_examples;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/***
 * 
 * 对输入数据进行排序
 * MapReduce过程中就有排序，是默认排序的。
 * 默认按照key值进行排序的。
 * 如果key为封装int的IntWritable类型，那么MapReduce按照数字大小对key排序
 * 如果key为封装String的Text类型，那么MapReduce按照字典顺序对字符串排序
 * 
 * @author qingjian
 *
 */
public class Sort {
	private static final String INPUT_PATH="hdfs://SparkMaster:9000/eclipse/sort";
	private static final String OUTPUT_PATH="hdfs://SparkMaster:9000/out";
	final static String RESULT = "hdfs://SparkMaster:9000/out/part-r-00000";
	//map将输入中的value转换成IntWritable类型，作为输出的key
	public static class MyMap extends Mapper<Object, Text, IntWritable, IntWritable> {
		private static IntWritable data = new IntWritable();
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//value是读取一行数据
			String line = value.toString();
			System.out.println("map->line="+line);
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}
	}
	
	//reduce将输入中的key复制到输出数据的key上，
	//然后根据输入的value-list中元素的个数决定key的输出次数
	//用全局linenum来代表key的位次
	public static class MyReduce extends Reducer<IntWritable, IntWritable,IntWritable, IntWritable> {
		private static IntWritable linenum = new IntWritable(1);
		@Override
		protected void reduce(
				IntWritable key,
				Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("reduce->key="+key);
			for (IntWritable val : values) {
				System.out.println("reduce->val="+val);
				context.write(linenum, key);
				linenum = new IntWritable(linenum.get()+1);
			}
		}
		
	}
	
	public static void main(String args[]) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		check(fileSystem);
		
		Job job = new Job(conf,Uniq.class.getSimpleName());
		
		//设置Map和Reduce处理类
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		//设置输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		//设置输入和输出目录
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		
		job.waitForCompletion(true);
		
		//控制台打印输出结果
		output(fileSystem,conf);
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
			
			FSDataInputStream in = fileSystem.open(new Path(RESULT));
			IOUtils.copyBytes(in, System.out, conf);
		}
	
}
