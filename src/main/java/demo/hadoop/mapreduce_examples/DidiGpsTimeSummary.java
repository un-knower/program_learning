package demo.hadoop.mapreduce_examples;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class DidiGpsTimeSummary {
	private static String input1=null;
	private static String input2=null;
	private static String output=null;

	public static CommandLine getCommand(String[] args) throws ParseException {
		CommandLineParser commandLineParse = new BasicParser();
		Options options = new Options();
		options.addOption("i1", true, "input info path from hadoop");
		options.addOption("i2", true, "input info path from mysql");
		options.addOption("o", true, "output data path");
		return commandLineParse.parse(options, args);
	}

	public static void deleteDocument(FileSystem fileSystem,String path) throws IOException {
		if(fileSystem.exists(new Path(path))) {
			fileSystem.delete(new Path(path), true);
		}
	}
	/**
	 * 将路径列表以逗号分割，返回该路径字符串
	 * @param listPath
	 * @return
	 */
	public static String splitPathWithComma(List<Path> listPath) {
		StringBuffer inputPath=new StringBuffer();
		if(listPath.size()>0) {
			inputPath.append(listPath.get(0));
			for(int i=1;i<listPath.size();i++) {
				inputPath.append(","+listPath.get(i));
		}
			return inputPath.toString();
		}
		return null;
		
	}
	public static void main(String[] args) throws ParseException, IOException {
		List<Path> listPath1 = new ArrayList<Path>();
		List<Path> listPath2 = new ArrayList<Path>();
				
		args = new String[6];
		args[0]="-i1";
		args[1]="hdfs://localhost:9000/eclipse/phone_gps_time"; 
		args[2]="-i2";
		args[3]="hdfs://localhost:9000/eclipse/d_info";
		args[6]="-o";
		args[7]="hdfs://localhost:9000/out";
		
		
		CommandLine commandLine = getCommand(args);
		input1=commandLine.getOptionValue("i1");
		input2=commandLine.getOptionValue("i2");
		output=commandLine.getOptionValue("o");

		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(input1), conf);
		if(fileSystem.exists(new Path(output))) {
			Scanner s = new Scanner(System.in); 
			System.out.println("Output directory "+output+" already exists,do you want to delete it(y/n)?");
			String choice =s.nextLine();
			if("y".equalsIgnoreCase(choice)) {
				deleteDocument(fileSystem,output);
			}else {
				System.out.println("shut down!");
				System.exit(0);
			}
		}
		
		
		Job job = new Job(conf);
		
		
		//MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, mapFromHadoop.class);
		
		
		
		
	}
	static class mapFromHadoop extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			
			
			
			
		}
		
	}
	static class mapFromMySql extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		}
		
	}
}
