package demo.hadoop.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySort {
	private static final Path INPUT_PATH= new Path("hdfs://sparkmaster:9000/eclipse/secondary");
	private static final Path OUTPUT_PATH = new Path("hdfs://sparkmaster:9000/out");
	private static final Path RESULT_PATH = new Path("hdfs://sparkmaster:9000/out/part-r-00000");
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(INPUT_PATH.toUri(),conf);
		
		if(!fileSystem.exists(INPUT_PATH)) {
			System.err.println("Not Exists DataSource...");
		}
		if(fileSystem.exists(OUTPUT_PATH)) {
			fileSystem.delete(OUTPUT_PATH, true);
		}
		
		Job job = new Job(conf, SecondarySort.class.getSimpleName());
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(4);  //设置4个分片
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, OUTPUT_PATH);
		if(job.waitForCompletion(true)) {
			FSDataInputStream in = fileSystem.open(RESULT_PATH);
			IOUtils.copyBytes(in, System.out, conf);
		}
	}
	static class MyMap extends Mapper<LongWritable, Text, IntPair, NullWritable> {
		IntPair intkey = new IntPair();
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, IntPair, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] splits = value.toString().split(" ");
			intkey.set(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]));
			context.write(intkey, NullWritable.get());
		}
		
	}
	static class MyReduce extends Reducer<IntPair, NullWritable, IntWritable, IntWritable> {

		@Override
		protected void reduce(
				IntPair key,
				Iterable<NullWritable> values,
				Reducer<IntPair, NullWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new IntWritable(key.first), new IntWritable(key.second));
		}
		
	}
	static class IntPair implements WritableComparable<IntPair> {
		int first;
		int second;
		public IntPair(){
		}
		public IntPair(int first, int second) {
			this.first = first;
			this.second = second;
		}
		public void set(int first, int second) {
			this.first = first;
			this.second = second;
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(first);  //out.writeInt 不报错，但是 map100% reduce0%
			out.writeInt(second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first = in.readInt();
			this.second = in.readInt();
		}

		@Override
		public int compareTo(IntPair o) {
			int minus = first - o.first;
			if(minus != 0) {
				return minus;
			}
			return second - o.second;
		}
		//HashPartitioner使用hasCode()方法来选择reduce分区
//		@Override
		public int hashCode() {
			return new Integer(first).hashCode();
		}
//		
//		@Override
//		public boolean equals(Object obj) {
//			if(!(obj instanceof IntPair)){
//				return false;
//			}
//			IntPair oK2 = (IntPair)obj;
//			return (this.first==oK2.first)&&(this.second==oK2.second);
//		}
		
	}
}
