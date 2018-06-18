package demo.hadoop.sample;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public final class SamplerJob {
  public static void main(String... args) throws Exception {
    runSortJob(args[0], args[1]);
  }

  public static void runSortJob(String input, String output)
      throws Exception {
    Configuration conf = new Configuration();

    Job job = new Job(conf);
    job.setJarByClass(SamplerJob.class);

    ReservoirSamplerInputFormat.setInputFormat(job, //这个是唯一需要调用的方法，通过他设置InputFormat从数据源中读取记录
        TextInputFormat.class);

    ReservoirSamplerInputFormat.setNumSamples(job, 10); //设置提取的样本数量N，这个数量N可以是所有inputsplit的样本总数，也可以是每个inputsplit都取N个样本。可以通过setUseSamplesNumberPerInputSplit方法设置该行为
    ReservoirSamplerInputFormat.setMaxRecordsToRead(job, 10000); //设置需要在每个InputSplit中读取的用于创建样本的记录的最大量。
    ReservoirSamplerInputFormat.
        setUseSamplesNumberPerInputSplit(job, true);//确定样本从所有inputsplit中总共提取N个样本，还是从每个InputSplit中都提取N个样本。如果设置为false（默认情况下），inputsplit的数量应该是样本数量的倍数

    Path outputPath = new Path(output);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, outputPath);

    outputPath.getFileSystem(conf).delete(outputPath, true);

    job.waitForCompletion(true);
  }
}
