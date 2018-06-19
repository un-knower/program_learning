package demo.hadoop.io.output.test;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class MyMap extends Mapper<LongWritable, Text, Text, BytesRefArrayWritable> {

	Text mapKey = new Text();

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, Text, BytesRefArrayWritable>.Context context)
			throws IOException, InterruptedException {
		if(StringUtils.isNotBlank(value.toString())) {
			String[] split = value.toString().split(",");
			mapKey.set(split[0]);
			context.write(mapKey, BytesRefUtil.createRcOutValue(value.toString(),","));
		}
	}
	
	
}
public class MultipleOutputFoldSina {
	private static final String INPATH = "hdfs://SparkMaster:9000/eclipse/products";
	private static final String OUTPATH = "hdfs://SparkMaster:9000/out";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(INPATH), conf); 
		if(fileSystem.exists(new Path(OUTPATH))) {
			fileSystem.delete(new Path(OUTPATH),true);
		}
		
		Job job = new Job(conf, "");
		job.setMapperClass(MyMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesRefArrayWritable.class);
		job.setJarByClass(MultipleOutputFoldSina.class);
		job.setNumReduceTasks(0);
		SinaMultipleTextOutputFormat.setColumnNumber(conf, 0);
		job.setOutputFormatClass(SinaMultipleTextOutputFormat.class); ////////
		FileInputFormat.setInputPaths(job, INPATH);
		FileOutputFormat.setOutputPath(job, new Path(OUTPATH));
		job.waitForCompletion(true);
		
		
	}
	
	
	
}
