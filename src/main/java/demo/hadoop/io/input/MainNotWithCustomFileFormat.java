package demo.hadoop.io.input;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainNotWithCustomFileFormat {
	
	static final String INPUT_PATH = "hdfs://master:9000/eclipse/phone_gps_time";
	static final String OUTPUT_PATH = "hdfs://master:9000/out";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(INPUT_PATH), conf);
		check(fileSystem);
		
		Job job = new Job(conf, "NotWithCustomFileFormat");
		job.setMapperClass(MapNotWithCustomFileFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		//
		job.setInputFormatClass(TextInputFormat.class); //采用系统默认的读入格式
		
		job.waitForCompletion(true);
		
	}
	
	
	public static void check(FileSystem fileSystem) throws IOException {
        if(fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }
        if(!fileSystem.exists(new Path(INPUT_PATH))) {
            System.err.println("Usage: Data Source not Found");
            System.exit(1);
        }
 }
}
