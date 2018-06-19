package demo.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CSVMapReduce {
	static class Map extends Mapper<LongWritable, TextArrayWritable, 
		LongWritable, TextArrayWritable> {
		@Override
		protected void map(LongWritable key, TextArrayWritable value,
				Mapper<LongWritable, TextArrayWritable, LongWritable, TextArrayWritable>.Context context)
						throws IOException, InterruptedException {
			context.write(key, value);
		}
		
	}
	static class Reduce extends Reducer<LongWritable, TextArrayWritable, TextArrayWritable, NullWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<TextArrayWritable> values,
				Reducer<LongWritable, TextArrayWritable, TextArrayWritable, NullWritable>.Context context)
						throws IOException, InterruptedException {
			for (TextArrayWritable val : values) {
				context.write(val, NullWritable.get());
			}
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String INPUT = "hdfs://master:9000/data/csv";
		final String OUTPUT = "hdfs://master:9000/output";
		
		Configuration conf = new Configuration();
		conf.set(CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG, ",");  //<co id="ch03_comment_csv_mr3"/>
	    conf.set(CSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG, "."); //<co id="ch03_comment_csv_mr4"/>
	    
	    Job job = new Job(conf);
	    job.setJarByClass(CSVMapReduce.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(TextArrayWritable.class);
	    job.setOutputKeyClass(TextArrayWritable.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    job.setInputFormatClass(CSVInputFormat.class);
	    job.setOutputFormatClass(CSVOutputFormat.class);
	    FileInputFormat.setInputPaths(job, INPUT);
	    FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
	    
	    job.waitForCompletion(true);
	    
	}
}
