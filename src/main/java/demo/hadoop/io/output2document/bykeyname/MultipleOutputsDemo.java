package demo.hadoop.io.output2document.bykeyname;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 输出到多个文件或多个文件夹，文件夹名称按key取
 * @author qingjian
 *
 */
class MultipleOutPutMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		if(StringUtils.isNotBlank(line)) {
			String[] split = line.split(",");
			
			context.write(new IntWritable(Integer.parseInt(split[0])), value);
		}
	
	}
}

class MultipleOutPutReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> multipleOutputs = null; //输出类型与该类的输出类型一致
	
	@Override
	protected void setup(
			Reducer<IntWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		
	}
	
	@Override
	protected void cleanup(
			Reducer<IntWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		if(multipleOutputs != null) {
			multipleOutputs.close();
			multipleOutputs = null;
		}
		
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		for (Text text : values) {
								//namedOutput, key, value, baseOutputPath
			multipleOutputs.write("KeySplit", NullWritable.get(), text, key.toString()+"/part"); //输出为 key.toStirng/part-r-nnnn
			multipleOutputs.write("AllPart", NullWritable.get(), text); //么有baseOutputPath 生成文件为  `namedOutput`-r-00000
			
		}
		
		
		

	}

	
	
	
}



public class MultipleOutputsDemo {
	private static final String INPUT_PATH = "hdfs://SparkMaster:9000/eclipse/products";
	private static final String OUTPUT_PATH = "hdfs://SparkMaster:9000/out";
										       
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH));
		}
		
		Job job = new Job(conf,"job name");
		
		job.setJarByClass(MultipleOutputsDemo.class);
		job.setMapperClass(MultipleOutPutMapper.class);
		job.setReducerClass(MultipleOutPutReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		
		MultipleOutputs.addNamedOutput(job, "KeySplit", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "AllPart", TextOutputFormat.class, NullWritable.class, Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
		
	}
}
