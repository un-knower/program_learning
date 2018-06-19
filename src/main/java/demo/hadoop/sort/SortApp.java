package demo.hadoop.sort;

import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//Caused by: java.lang.NoSuchMethodException: sort.SortApp$NewK2.<init>()

//上面这个错误是没有写NewK2的无参构造方法
import demo.hadoop.suanfa.TopKApp;
/**
 * hadoop 二级排序
 *#首先按照第一列升序排列，当第一列相同时，第二列升序排列
 *输入
	3	3
	3	2
	3	1
	2	2
	2	1
	1	1
 *输出
	1	1
	2	1
	2	2
	3	1
	3	2
	3	3
 *
 *
 */
public class SortApp {
	static final Path INPUT_PATH = new Path("hdfs://localhost:9000/eclipse/data");
	static final Path OUTPUT_PATH = new Path("hdfs://localhost:9000/out");
	static final Path RESULT_PATH = new Path("hdfs://localhost:9000/out/part-r-00000");
	public static void check(FileSystem fileSystem) throws IOException {
		if(!fileSystem.exists(INPUT_PATH)) {
			System.err.println("Usage:Data Source not Found!");
			System.exit(1);
		}
		if(fileSystem.exists(OUTPUT_PATH)) {
			fileSystem.delete(OUTPUT_PATH, true);
		}
	}
	public static void output(Configuration conf, FileSystem fileSystem) throws IOException {
		FSDataInputStream in = fileSystem.open(RESULT_PATH);
		IOUtils.copyBytes(in, System.out, conf);
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(INPUT_PATH.toUri(), conf);
		
		check(fileSystem);
		
		Job job = new Job(conf,TopKApp.class.getSimpleName());
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, OUTPUT_PATH);
		if(job.waitForCompletion(true)) {
			output(conf, fileSystem);
		}
	}
	static class MyMap extends Mapper<LongWritable, Text, NewK2, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NewK2, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			NewK2 newK2 = new NewK2(Long.parseLong(split[0]), Long.parseLong(split[1]));
			context.write(newK2, NullWritable.get());
		}
		
	}
	
	static class MyReduce extends Reducer<NewK2, LongWritable, LongWritable, LongWritable> {

		@Override
		protected void reduce(
				NewK2 k2,
				Iterable<LongWritable> v2s,
				Reducer<NewK2, LongWritable, LongWritable, LongWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(k2.first), new LongWritable(k2.second));
		}
		
	}
	/**
	 * 问：为什么实现该类？
	 * 答：因为原来的v2不能参与排序，把原来的k2和v2封装到一个类中，作为新的k2
	 * 
	 * 问：java自带序列化框架，为什么Hadoop要重新开发
	 * 答：	1)java的序列化机制在每个类的对象第一次出现的时候保存了每个类的信息，比如类名，第二次出现的类的对象会有一个类的reference，导致空间的浪费
	 * 		2)Java序列化机制不能复用对象。在Hadoop的序列化机制中，反序列化的对象是可以复用的。
	 *
	 *WritableComparable接口是Writable和java.lang.Comparable接口的子接口
	 */
	static class NewK2 implements WritableComparable<NewK2> {
		Long first;
		Long second;
		public NewK2(){}
		public NewK2(long first, long second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
			out.writeLong(second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
			this.second = in.readLong();
		}
		
		/**
		 * 当k2进行排序时，会调用该方法.
		 * 当第一列不同时，升序；当第一列相同时，第二列升序
		 */
		@Override
		public int compareTo(NewK2 o) {
			long minus = this.first - o.first;
			if(minus!=0) {
				return (int) minus;
			}
			return (int) (this.second-o.second);
		}
		//HashPartitioner使用hasCode()方法来选择reduce分区
//		@Override
//		public int hashCode() {
//			return this.first.hashCode()+this.second.hashCode();
//		}
//		
//		@Override
//		public boolean equals(Object obj) {
//			if(!(obj instanceof NewK2)){
//				return false;
//			}
//			NewK2 oK2 = (NewK2)obj;
//			return (this.first==oK2.first)&&(this.second==oK2.second);
//			return (this.first.equals(oK2.first))&&(this.second.equals(oK2.second));		
//		}
	}
}




