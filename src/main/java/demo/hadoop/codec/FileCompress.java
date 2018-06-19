package codec;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * MapReduce的输出进行压缩
 * 
 * 减少map与reducer之间数据量的传递：
 * (1)使用combine进行map端数据的合并
 * (2)map端数据进行压缩     conf.setBoolean("mapred.compress.map.output", true);
 * 设置map端数据压缩的算法
 * conf.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class);
 * 
 *   
 * 设置reduce端数据的压缩
 * conf.setBoolean("mapred.output.compress", true);
 * 数据压缩算法
 * conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
 * 
 * 
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
	private FileSplit fileSplit; //当前文件的分片：可以得到该文件的名称和路径
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		fileSplit = (FileSplit) context.getInputSplit();
		StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
		while(stringTokenizer.hasMoreElements()) {
			keyInfo.set(stringTokenizer.nextToken()+":"+fileSplit.getPath().getName());  //word:filename
			valueInfo.set("1"); //1
			context.write(keyInfo, valueInfo);
		}
		
	}
	
}
/**
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * @author qingjian
 * Combiner对map的数据进行整理，减少map与reduce之间的数据传递
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
		keyInfo.set(split[0]); //word
		valueInfo.set(split[1]+":"+sum); //filename:num
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
		StringBuffer strBuf = new StringBuffer(); //filename1:num1;filename2:num2
		for (Text text : values) { 
			strBuf.append(text.toString()).append(";");
		}
		valueInfo.set(strBuf.toString());
		context.write(key, valueInfo);
	}
	
}

public class FileCompress {
	static final String INPUT_PATH="hdfs://SparkMaster:9000/eclipse/index_in";
	static final String OUTPUT_PATH="hdfs://SparkMaster:9000/out";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		/**
		 * 压缩代码
		 */
		//map端输出进行压缩
		conf.setBoolean("mapred.compress.map.output", true);
		//reduce端输出进行压缩
		conf.setBoolean("mapred.output.compress", true);
		//reduce端输出压缩使用的类
		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		
		
		FileSystem fileSystem = FileSystem.get(URI.create(INPUT_PATH), conf);
		if(!fileSystem.exists(new Path(INPUT_PATH))) {
			System.err.print("");
			System.exit(1); //非正常退出
		}
		if(fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		Job job = new Job(conf,FileCompress.class.getSimpleName());
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
