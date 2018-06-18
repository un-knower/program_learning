package demo.hadoop.io;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Elephant Bird的LzoJsonInputFormat中的输入格式创建一个能够处理JSON元素的输入格式类
 * 但是要求输入的文件必须是LOZP压缩文件
 * 
 * 自行设计的JsonInputFormat可以读取json，但是必须每行是一个JSON对象
 * @author qingjian
 *
 */
public class JsonRead {
	static class JsonMap extends Mapper<LongWritable, MapWritable, Text, Text> {

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, MapWritable value,
				Mapper<LongWritable, MapWritable, Text, Text>.Context context)
						throws IOException, InterruptedException {
			Set<Entry<Writable, Writable>> entrySet = value.entrySet();
			for (Entry<Writable, Writable> entry : entrySet) {
				context.write((Text)entry.getKey(), (Text)entry.getValue());
			}
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String INPUT = "hdfs://master:9000/data/json";
		String OUTPUT = "hdfs://master:9000/output";
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(JsonRead.class);
		job.setMapperClass(JsonMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(JsonInputFormat.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(INPUT));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
		
		job.waitForCompletion(true);
		
		
		
	}
}
