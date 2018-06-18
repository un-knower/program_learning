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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 手机上网日志处理 计算每个电话的上网流量
 * 
 * @author qingjian
 *
 */
public class KpiApp {
	private static final String INPUT_PATH = "hdfs://localhost:9000/eclipse/wlan";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/out";

	// 检测输入/输出路径
	public static void check(FileSystem fileSystem) throws IOException {
		if (!fileSystem.exists(new Path(INPUT_PATH))) {
			System.err.println("Usage: Data Source not Found");
			System.exit(2); // 非正常关闭
		}
		if (fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
	}

	// 显示输出结果
	public static void output(FileSystem fileSystem, Configuration conf)
			throws IOException {
		final String RESULT = "hdfs://localhost:9000/out/part-r-00000";
		FSDataInputStream in = fileSystem.open(new Path(RESULT));
		IOUtils.copyBytes(in, System.out, conf);
	}

	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf,"Hadoop");
		check(fileSystem);
		
		Job job = new Job(conf,KpiApp.class.getSimpleName());
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置输入输出路径
		//ERROR security.UserGroupInformation: PriviledgedActionException as:qingjian cause:org.apache.hadoop.mapred.InvalidJobConfException: Output directory not set.
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		if(job.waitForCompletion(true)) {
			output(fileSystem, conf);
		}
		
	}
	
	public static class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text keyInfo = new Text();
		private IntWritable valueInfo = new IntWritable();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] content = value.toString().split("\t");
			//content[1] ：手机号码
			//content[6] ：上行数据包数
			//content[7] ：下行数据包数
			//content[8] ：上行总流量
			//content[9] ：下行总流量
			
			keyInfo.set(content[1]);
			int upPayLoad = Integer.parseInt(content[8]);
			int downPayLoad = Integer.parseInt(content[9]);
			valueInfo.set(upPayLoad+downPayLoad);
			context.write(keyInfo, valueInfo);
		}
	}
	public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum=0;
			for (IntWritable val : value) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
