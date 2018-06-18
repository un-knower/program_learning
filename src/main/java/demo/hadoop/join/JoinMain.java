package demo.hadoop.join;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinMain {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String inpath = "hdfs://master:9000/data/hive/join";
		Path outputpath = new Path("hdfs://master:9000/out");
		
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(inpath),conf);
		if(fileSystem.exists(outputpath)) {
			fileSystem.delete(outputpath, true);
		}
		Job job = new Job(conf,"join");
		
		job.setJarByClass(JoinMain.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Employee.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, inpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		
		job.waitForCompletion(true);
	}
}
