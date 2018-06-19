package demo.hadoop.mapreduce_examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KpiApp2 {
	private static final String INPUT_PATH = "hdfs://localhost:9000/eclipse/wlan";
	private static final String OUTPUT_PATH = "hdfs://localhost:9000/out";
	public static void check(FileSystem fileSystem) throws IOException {
		if(fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		if(!fileSystem.exists(new Path(INPUT_PATH))) {
			System.err.println("Usage: Data Source not Found");
			System.exit(1);
		}
	}

	// 显示输出结果
	public static void output(FileSystem fileSystem, Configuration conf)
			throws IOException {
		final String RESULT = "hdfs://localhost:9000/out/part-r-00000";
		FSDataInputStream in = fileSystem.open(new Path(RESULT));
		IOUtils.copyBytes(in, System.out, conf);
	}

	public static void main(String[] args) throws IOException,URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		check(fileSystem);
		Job job = new  Job(conf,KpiApp2.class.getSimpleName());
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		//
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable.class);
		//设置输入/输出路径
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		if(job.waitForCompletion(true)) {
			output(fileSystem, conf);
		}
		
	}
	public static class MyMap extends Mapper<LongWritable, Text, Text, KpiWritable> {
		private Text keyInfo = new Text();
		private KpiWritable valueInfo = null;
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, KpiWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			//split[1] ：手机号码
			//split[6] ：上行数据包数
			//split[7] ：下行数据包数
			//split[8] ：上行总流量
			//split[9] ：下行总流量
			keyInfo.set(split[1]);
			valueInfo = new KpiWritable(split[6], split[7], split[8], split[9]);
			context.write(keyInfo, valueInfo);
			
		}
		
	}
	public static class MyReduce extends Reducer<Text, KpiWritable, Text, KpiWritable> {
		@Override
		protected void reduce(Text key, Iterable<KpiWritable> values,
				Reducer<Text, KpiWritable, Text, KpiWritable>.Context context)
				throws IOException, InterruptedException {
			long upPackNumSum = 0L;
			long downPackNumSum = 0L;
			long upPayLoadSum = 0L;
			long downPayLoadSum = 0L;
			for (KpiWritable val : values) {
				upPackNumSum += val.upPackNum;
				downPackNumSum += val.downPackNum;
				upPayLoadSum += val.upPayLoad;
				downPayLoadSum += val.downPayLoad;
			}
			context.write(key, new KpiWritable(upPackNumSum, downPackNumSum, upPayLoadSum, downPayLoadSum));
		}
		
	}
}

//Hadoop自定义类型需要实现Writable接口
class KpiWritable implements Writable {
	
	long upPackNum;
	long downPackNum;
	long upPayLoad;
	long downPayLoad;
	
	public KpiWritable() {

	}
	public KpiWritable(String upPackNum, String downPackNum, String upPayLoad, String downPayLoad) {
		this.upPackNum = Long.parseLong(upPackNum);
		this.downPackNum = Long.parseLong(downPackNum);
		this.upPayLoad = Long.parseLong(upPayLoad);
		this.downPayLoad = Long.parseLong(downPayLoad);
	}
	public KpiWritable(Long upPackNum, Long downPackNum, Long upPayLoad, Long downPayLoad) {
		this.upPackNum = upPackNum;
		this.downPackNum = downPackNum;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upPackNum);
		out.writeLong(downPackNum);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readLong();
		this.downPackNum = in.readLong();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();		
	}
	@Override
	public String toString() {
		return upPackNum+"\t"+downPackNum+"\t"+upPayLoad+"\t"+downPayLoad;
	}
}
