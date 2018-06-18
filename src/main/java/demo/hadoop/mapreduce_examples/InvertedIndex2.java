package mapreduce_examples;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * 根据多个文件构建倒俳索引(精简版)
 * @author qingjian
 *
 */
/**
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * @author qingjian
 *
 */
class MyMap extends Mapper<Object,Text,Text,Text> {

	private Text keyInfo = new Text();
	private Text valueInfo = new Text();
	private FileSplit fileSplit;
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		fileSplit = (FileSplit) context.getInputSplit();
		StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
		while(stringTokenizer.hasMoreElements()) {
			keyInfo.set(stringTokenizer.nextToken()+":"+fileSplit.getPath().getName());
			valueInfo.set("1");
			context.write(keyInfo, valueInfo);
		}
		
	}
	
}
/**
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * @author qingjian
 *
 */
class MyCombiner extends Reducer<Text,Text,Text,Text> {
	private Text keyInfo = new Text();
	private Text valueInfo = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Long sum = 0L;
		for (Text count : values) {
			sum += Long.parseLong(count.toString());
		}
		String[] split = key.toString().split(":");
		keyInfo.set(split[0]);
		valueInfo.set(split[1]+":"+sum);
		context.write(keyInfo, valueInfo);
	}
	
}

/**
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * @author qingjian
 *
 */
class MyReducer extends Reducer<Text,Text,Text,Text> {
	private Text valueInfo = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		StringBuffer strBuf = new StringBuffer();
		for (Text text : values) {
			strBuf.append(text.toString()).append(";");
		}
		valueInfo.set(strBuf.toString());
		context.write(key, valueInfo);
	}
	
}

public class InvertedIndex2 {
	static final String INPUT_PATH="hdfs://localhost:9000/eclipse/index_in";
	static final String OUTPUT_PATH="hdfs://localhost:9000/out";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(INPUT_PATH), conf);
		if(!fileSystem.exists(new Path(INPUT_PATH))) {
			System.err.print("");
			System.exit(1); //非正常退出
		}
		if(fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		Job job = new Job(conf,InvertedIndex2.class.getSimpleName());
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MyMap.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		
		job.waitForCompletion(true);
	}
	
	
}
