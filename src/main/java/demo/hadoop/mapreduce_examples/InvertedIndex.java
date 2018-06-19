package demo.hadoop.mapreduce_examples;

import java.io.IOException;

import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 根据多个文件构建倒俳索引
 * @author qingjian
 *
 */
public class InvertedIndex {
	final static String INPUT_PATH="hdfs://localhost:9000/eclipse/index_in";
	final static String OUTPUT_PATH="hdfs://localhost:9000/out";
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		//这句话很关键
		//conf.set("mapred.job.tracker", "localhost:9001");加上这句话报错！！！
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		
		if(!fileSystem.exists(new Path(INPUT_PATH))) {
			System.err.println("Usage: Inverted Index<in> <out>");
			/**
			 * 
    		 *	System.exit(0)是将你的整个虚拟机里的内容都停掉了 ，而dispose()只是关闭这个窗口，但是并没有停止整个application exit() 。无论如何，内存都释放了！也就是说连JVM都关闭了，内存里根本不可能还有什么东西
    		 *	System.exit(0)是正常退出程序，而System.exit(1)或者说非0表示非正常退出程序
    		 *	System.exit(status)不管status为何值都会退出程序。和return 相比有以下不同点：return是回到上一层，而System.exit(status)是回到最上层
			 */
			System.exit(2);
		}
		if(fileSystem.exists(new Path(OUTPUT_PATH))) {
			fileSystem.delete(new Path(OUTPUT_PATH),true);
		}
		
		
		Job job = new Job(conf,"Inverted Index");
		job.setJarByClass(InvertedIndex.class);
		
		//设置Map、Combine和Reduce处理类
		job.setMapperClass(MyMap.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		//设置Map输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//设置Reduce输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		System.exit(job.waitForCompletion(true)?0:1); //0表示正常结束，非零表示非正常结束
	}
	/**
	 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
	 * @author qingjian
	 *
	 */
	static class MyMap extends Mapper<Object,Text,Text,Text> {
		
		private Text keyInfo = new Text();
		//存储单词和URL组合
		private Text valueInfo = new Text();
		//存储Split对象
		private FileSplit split;
		//实现map函数
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("map->key: "+key+",value: "+value);
			//获得<key,value>对所属的FileSplit对象
			split = (FileSplit) context.getInputSplit();
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreElements()) {
				//key值由单词和URL组成，如"MapReduce:file1.txt"
				//获取文件的完整路径
				//keyInfo.set(itr.nextToken()+":"+split.getPath().toString());
				//这里为了好看，只获取文件的名称
				int splitIndex = split.getPath().toString().indexOf("file");//split.getPath().getName()
				keyInfo.set(itr.nextToken()+":"+split.getPath().toString().substring(splitIndex));
				//词频初始化为1
				valueInfo.set("1");
				context.write(keyInfo, valueInfo);
			}
			
		}
		
	}
	
	
	/**
	 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
	 * @author qingjian
	 *
	 */
	static class Combine extends Reducer<Text,Text,Text,Text> {
		
		private Text info = new Text();
		//实现reduce函数
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("combine->key: "+key);
			//统计词频
			int sum=0;
			for(Text text : values) {
				sum += Integer.parseInt(text.toString());
			}
			int splitIndex = key.toString().indexOf(":");
			//重新设置value值由URL和词频组成
			info.set(key.toString().substring(splitIndex+1)+":"+sum);
			//重新设置key值为单词
			key.set(key.toString().substring(0,splitIndex));
			context.write(key, info);
		}
		
	}
	
	
	/**
	 * 
	 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
	 * @author qingjian
	 *
	 */
	static class Reduce extends Reducer<Text,Text,Text,Text> {
		
		private Text result = new Text();
		//实现reduce函数
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("reduce->key: "+key);
			//生成文档列表
			String fileList = new String();
			for(Text value:values) {
				fileList+=value.toString()+";";
			}
			result.set(fileList);
			context.write(key, result);
		}
		
	}
}
