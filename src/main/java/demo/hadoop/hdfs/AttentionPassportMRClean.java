package demo.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 数据清洗
 * 循环访问文件路径
 * 循环范文文件内的所有的文件
 * 
 * MR读取hdfs文件内容
 * 
 * @author qingjian
 *
 */
public class AttentionPassportMRClean {
	final static String inpath = "hdfs://sohuvideo-dm-master/tmp/test";
	final static String outpath = "hdfs://sohuvideo-dm-master/tmp/test/output";
	static List<Path> inputFiles = new ArrayList<Path>();
	private static Path path;
	private static Counter counter;
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(inpath), conf);
		if(fileSystem.exists(new Path(inpath))) {
			System.out.println("yes");
		}
		else {
			System.out.println("no");
		}
		FileStatus[] listStatus = fileSystem.listStatus(new Path(inpath));
		for(FileStatus fileStatus:listStatus) {
			System.out.println(fileStatus.getPath());
			if(!fileStatus.isDir()) {
				inputFiles.add(fileStatus.getPath());
			}
		}
		Job job= new Job(conf);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MyMapper.class);
		//job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job,new Path(inpath ));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		job.waitForCompletion(true);
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable key = new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			counter = context.getCounter("Index", "num");
			counter.increment(1);
			key.set(counter.getValue());
			
			context.write(key, new Text(value.toString().replaceAll("\"", "").replaceAll("Z", "").replaceAll("T", " ")));
			
		}
		
	}
//	static class MyReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
//
//		@Override
//		protected void reduce(LongWritable key, Iterable<Text> values,
//				Context context)
//				throws IOException, InterruptedException {
//			
//			
//			
//		}
//		
//	}

}
