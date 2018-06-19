package demo.hadoop.join.mapjoin;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author qingjian
 
 EMP 表
 
 Name	Sex		Age		DepNo
 zhang	male	20		1
 li		female	25		2
 wang	female	30		3
 zhou	male	35		2
 
 DEP 表
 
 DepNo	DepName
 1		Sales
 2		Dev
 3		Mgt
 
 *
 *
 *
 */

public class MapJoinMain {
	private static final String INPUT = "hdfs://master:9000/eclipse/mapjoin/emp.txt";
	private static final String DISTRIBUTED_INPUT = "hdfs://master:9000/eclipse/mapjoin/dept.txt";
	private static final String OUTPUT = "hdfs://master:9000/out";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(URI.create(INPUT), conf);
		if(fileSystem.exists(new Path(OUTPUT))) {
			fileSystem.delete(new Path(OUTPUT), true);
		}
		
		Job job = new Job(conf,"map join");
		job.setMapperClass(MapJoinMap.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(EMP_DEP.class);
		job.setNumReduceTasks(0);
		job.setJarByClass(MapJoinMain.class);
		job.addCacheFile(new Path(DISTRIBUTED_INPUT).toUri());
		
		FileInputFormat.setInputPaths(job, INPUT);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
		job.waitForCompletion(true);
	}
}
