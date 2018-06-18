package demo.hadoop.join3;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 不同的输入文件，进入不同的Map函数
 * @author qingjian
 *
 */
public class Join3Main {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String inputFromUser = "hdfs://master:9000/data/hive/join3/user.txt";
		String inputFromPhone = "hdfs://master:9000/data/hive/join3/phone.txt";
		Path outputPath = new Path("hdfs://master:9000/out"); 
		Configuration conf = new Configuration();
		FileSystem fileSytem = FileSystem.get(URI.create(inputFromUser),conf);
		if(fileSytem.exists(outputPath)) {
			fileSytem.delete(outputPath);
		}
		
		Job job = new Job(conf,"join3");
		job.setJarByClass(Join3Main.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserPhone.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job,new Path(inputFromUser),TextInputFormat.class,Join3MapperFromUser.class);
		MultipleInputs.addInputPath(job,new Path(inputFromPhone),TextInputFormat.class,Join3MapperFromPhone.class);
		job.setReducerClass(Join3Reducer.class);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
	}
}
