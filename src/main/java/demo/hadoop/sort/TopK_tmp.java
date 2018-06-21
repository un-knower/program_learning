package demo.hadoop.sort;

import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * @author qingjian
 *
 * 唯一键
 * 每个map找出本地的top N列表，然后把他传递给一个reduce
 * 这个一个reduce找出所有top N列表汇总，找到最终的top n
 * 
 * 输入文件：
 * g	445
 * a	1117
 * b	222
 * c	333
 * d	444
 * e	123
 * f	345
 * h	456
 * 
 * 当k=2
 * 输出
 * 
	456	h
	1117	a

 * 
 * 因为使用了 TreeMap 所以要求比较的key 唯一！！！！！！！！
 * 如果key不唯一，那么需要配置TreeMap<Integer, List<String>> map = new TreeMap<Integer, List<String>>();
 * 相同的value写入同一个list里面
 *
 */


public class TopK_tmp {
	public static final int k = 2;
	public static final String INPUTPAHT = "hdfs://SparkMaster:9000/eclipse/topk/file1.txt";
//	public static final String INPUTPAHT = "hdfs://SparkMaster:9000/eclipse/topk"; //结果有问题了，当比较的key不唯一，就会出现问题
	public static final String OUTPUTPAHT = "hdfs://SparkMaster:9000/out";
	public static final String RESULT = "hdfs://SparkMaster:9000/out/part-r-00000";
	public static class KMap extends Mapper<LongWritable, Text, IntWritable, Text> {

		TreeMap<Integer, String> map = new TreeMap<Integer, String>(); //默认递增排序
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			
			if(line.trim().length()>0 && line.indexOf("\t")!=-1) {
				String[] split = value.toString().split("\t", 2);
				String name = split[0];
				Integer num = Integer.parseInt(split[1]);
				map.put(num, name);

				// 只保留k个数据，如果超出，则删除
				if(map.size()>k) {
					map.remove(map.firstKey()); // 删除最小的
				}
				
			}
			
		}

        /**
         * 在处理完所有数据后，才最终发送top n列表数据
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			for (int num : map.keySet()) {
				context.write(new IntWritable(num), new Text(map.get(num)));
			}
		
		}
		
		
		
		
	}
	
	public static class KReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		TreeMap<Integer, String> map = new TreeMap<Integer, String>(); //默认递增排序

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			map.put(key.get(), values.iterator().next().toString()); ///只能是比较数值是唯一的
			if(map.size()>k) {
				map.remove(map.firstKey());
			}
			
		}

		@Override
		protected void cleanup(
				Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for (int num : map.keySet()) {
				context.write(new IntWritable(num), new Text(map.get(num)));
			}
		}
		
	}
	
	
	
	
	
	public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		try {
			FileSystem fileSystem = FileSystem.get(URI.create(INPUTPAHT), conf);
			if(fileSystem.exists(new Path(OUTPUTPAHT))) {
				fileSystem.delete(new Path(OUTPUTPAHT), true);
			}
			
			Job job = new Job(conf);
			
			job.setMapperClass(KMap.class);
			job.setReducerClass(KReducer.class);
			job.setNumReduceTasks(1);  //  这里控制选1个reduce
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, INPUTPAHT);
			FileOutputFormat.setOutputPath(job, new Path(OUTPUTPAHT));
			if (job.waitForCompletion(true)) {
				output(fileSystem, conf);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		
	}
	//显示输出结果
	public static void output(FileSystem fileSystem,Configuration conf) throws IOException {
		
		FSDataInputStream in = fileSystem.open(new Path(RESULT));
		IOUtils.copyBytes(in, System.out, conf);
	}
}
