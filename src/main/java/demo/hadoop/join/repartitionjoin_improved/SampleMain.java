package demo.hadoop.join.repartitionjoin_improved;

import demo.hadoop.join.repartitionjoin.impl.CompositeKey;
import demo.hadoop.join.repartitionjoin.impl.CompositeKeyComparator;
import demo.hadoop.join.repartitionjoin.impl.CompositeKeyOnlyComparator;
import demo.hadoop.join.repartitionjoin.impl.CompositeKeyPartitioner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


/**
 * 
 * 合并两个文件
 * users.txt 用户文件
 
anne	22	NY
joe	39	CO
alison	35	NY
mike	69	VA
marie	27	OR
jim	21	OR
bob	71	CA
mary	53	NY
dave	36	VA
dude	50	CA

 * user-logs.txt 用户活动文件
 * 
jim	logout	93.24.237.12
mike	new_tweet	87.124.79.252
bob	new_tweet	58.133.120.100
mike	logout	55.237.104.36
jim	new_tweet	93.24.237.12
marie	view_user	122.158.130.90
jim	login	198.184.237.49
marie	login	58.133.120.100

 *
 *
 * 认为用户文件为小文件 
 * 实现的是inner join
 */



public class SampleMain {
  public static void main(String... args) throws Exception {

    JobConf job = new JobConf();
    job.setJarByClass(SampleMain.class);

    String input = args[0];
    Path output = new Path(args[1]);

    output.getFileSystem(job).delete(output, true);

    job.setMapperClass(SampleMap.class);
    job.setReducerClass(SampleReduce.class);

    job.setInputFormat(KeyValueTextInputFormat.class);

    job.setMapOutputKeyClass(CompositeKey.class);
    job.setMapOutputValueClass(TextTaggedOutputValue.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setPartitionerClass(CompositeKeyPartitioner.class);
    job.setOutputKeyComparatorClass(CompositeKeyComparator.class);
    job.setOutputValueGroupingComparator(
        CompositeKeyOnlyComparator.class);


    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    JobClient.runJob(job);
  }
}
