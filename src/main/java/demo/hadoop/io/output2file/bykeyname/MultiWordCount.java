package demo.hadoop.io.output2file.bykeyname;

import java.io.IOException;  
import java.net.URI;
import java.util.StringTokenizer;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  

/**
 *  文件内容：
 *  hello   world   a       abc     1253d
	world   word
 *  
 *  将统计结果按首字母开头分别进行文件保存
 *  
 */

public class MultiWordCount {  
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {  
        private final static IntWritable one = new IntWritable(1);  
        private Text word = new Text();  
        public void map(Object key, Text value, Context context) throws IOException,  
                InterruptedException {  
            StringTokenizer itr = new StringTokenizer(value.toString());  
            while (itr.hasMoreTokens()) {  
                word.set(itr.nextToken());  
                context.write(word, one);  
            }  
        }  
    }  
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {  
        private IntWritable result = new IntWritable();  
        public void reduce(Text key, Iterable<IntWritable> values, Context context)  
                throws IOException, InterruptedException {  
            int sum = 0;  
            for (IntWritable val : values) {  
                sum += val.get();  
            }  
            result.set(sum);  
            context.write(key, result);  
        }  
    }  
    public static class AlphabetOutputFormat extends MyMultipleOutputFormat<Text, IntWritable> {  
        @Override  
        protected String generateFileNameForKeyValue(Text key, IntWritable value, Configuration conf) {  
            char c = key.toString().toLowerCase().charAt(0);  
            if (c >= 'a' && c <= 'z') {  
                return c + ".txt";  
            }  
            return "other.txt";  
        }  
    }  
    private static final String INPUT_PATH="hdfs://SparkMaster:9000/eclipse/multiwordcount";
    private static final String OUTPUT_PATH="hdfs://SparkMaster:9000/out";
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
//        if (otherArgs.length != 2) {  
//            System.err.println("Usage: wordcount <in> <out>");  
//            System.exit(2);  
//        } 
        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		check(fileSystem);
        Job job = new Job(conf, "word count");  
        job.setJarByClass(MultiWordCount.class);  
        job.setMapperClass(TokenizerMapper.class);  
        job.setCombinerClass(IntSumReducer.class);  
        job.setReducerClass(IntSumReducer.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
        job.setOutputFormatClass(AlphabetOutputFormat.class);//设置输出格式  
        //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
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
