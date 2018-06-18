package demo.hadoop.hbase;
import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class ETLVideo {
	private static CommandLine getCommandLine(String[] args)
			throws ParseException {
		// 解析命令行
				CommandLineParser parser = new BasicParser();
				Options options = new Options();
				options.addOption("i", "input", true,"the directory or file to read from HDFS");
				options.addOption("t", "table", true, "table to import into");
				options.addOption("c", "column", true, "colum to store row data into");
				CommandLine commandLine = parser.parse(options, args);
				return commandLine;
		

	}
	static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable,Writable> {
		private byte[] family = null;
		private byte[] qualifier = null;
		public static Counter counter = null;
		@Override
		protected void setup(
				Mapper<LongWritable, Text, ImmutableBytesWritable,Writable>.Context context)
				throws IOException, InterruptedException {
			String column = context.getConfiguration().get("conf.column");
			byte[][] parseColumn = KeyValue.parseColumn(Bytes.toBytes(column));
			family = parseColumn[0];
			qualifier = parseColumn[1];
			System.out.println("family="+new String(family));
			System.out.println("qualifier="+new String(qualifier));
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, ImmutableBytesWritable,Writable>.Context context)
				throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\t");
			String rowkey = line[0];
			Put put = new Put(rowkey.getBytes());
			put.add(family, qualifier, line[1].getBytes());
			context.write(new ImmutableBytesWritable(rowkey.getBytes()), (Writable) put);
			counter = context.getCounter("content","rownum");
			counter.increment(1);
		}
		
	}

	public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException, InterruptedException {
		args[0]="-i";
		//args[1]="hdfs://localhost:9000/eclipse/codec/bigfile.gz";
		//args[1]="hdfs://localhost:9000/eclipse/shortModelInfo";
		args[1]="hdfs://localhost:9000/eclipse/pushVideo_uncompress";
		args[2]="-t";
		//args[3]="short_recommend";
		args[3]="hbase_t_m_user_shortmodel_pushvideo";
		args[4]="-c";
		//args[5]="videoinfo:tag";
		args[5]="push:vid";
		
		//Configuration 
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("dfs.socket.timeout", "180000");//将该值改大，防止hbase超时退出
		//Parse args
		CommandLine commandLine = getCommandLine(args);
		String INPUT_PATH = commandLine.getOptionValue("i");
		String TABLE_NAME = commandLine.getOptionValue("t");
		String COLUMN = commandLine.getOptionValue("c"); //family:colume
		System.out.println("input path="+INPUT_PATH);
		System.out.println("table name="+TABLE_NAME);
		System.out.println("colum="+COLUMN);
		conf.set("conf.column", COLUMN);
		//uncompress1(conf,INPUT_PATH);
		//HBaseAdmin
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(!admin.tableExists(TABLE_NAME)) { //如果表不存在
			//create hbase table
			HTableDescriptor htableDescriptor = new HTableDescriptor(TABLE_NAME);
			htableDescriptor.addFamily(new HColumnDescriptor(KeyValue.parseColumn(Bytes.toBytes(COLUMN))[0]));
			admin.createTable(htableDescriptor);
		}
		//Job
		Job job = new Job(conf,"Import from "+INPUT_PATH+" into table "+TABLE_NAME);
		//set jar
		job.setJarByClass(ETLVideo.class);
		//set map
		job.setMapperClass(ImportMapper.class);
		//set reduce 0
		job.setNumReduceTasks(0);
		//set tablename
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
		//conf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
		
		//set input format
		job.setInputFormatClass(TextInputFormat.class);
		//set output format
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setPartitionerClass(HashPartitioner.class);
		//set input path
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		//
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
