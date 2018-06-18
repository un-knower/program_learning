package demo.hadoop.demo.phone_gps;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class PhoneGps {
	private static String input1=null;
	private static String input2=null;
	private static String input3=null;
	private static String output=null;
	/*
	 * 统计信息counter 
	 */
	private static String GPS_Recod="GPS Record";
	private static String GPS_SUM="GPS total num";
	private static String GPS_NOTFOUND="GPS not found num";
	private static String GPS_FOUND="GPS found num";
	private static String GPS_Match="GPS dic match num"; 
	
	private static String DRIVER_SUM="driver total num";
	
	private static Counter counter_sum=null;
	private static Counter counter_notfound=null;
	private static Counter counter_found=null;
	private static Counter counter_match=null;
	private static Counter counter_drivernum=null;
	
	/*
	 * 全局变量不推荐使用
	private static long gps_sum = 0;
	private static long gps_notfound = 0;
	private static long gps_found = 0;
	private static long drivernum = 0;
	*/
	/*
	 * 将jons内容解析成基站key
	 */
	public static String jsonToKey(String key_json) {

		JSONObject jsonContent = JSON.parseObject(key_json);
		String dataContent = jsonContent.getString("data");
		String timeContent = jsonContent.getString("time");
		JSONObject dataJson = JSON.parseObject(dataContent);
		int mcc = Integer.parseInt(dataJson.getString("mcc"));
		int mnc = Integer.parseInt(dataJson.getString("mnc"));
		int lac = Integer.parseInt(dataJson.getString("lac"));
		int cid = Integer.parseInt(dataJson.getString("cid"));

		if (mnc >= 10000) {
			if (lac >= 0 && cid > 0) {
				return mnc + ", " + lac + ", " + cid+"\t"+timeContent;
			} else {
				return "";
			}
		}
		if (mcc < 0 || mnc < 0 || lac < 0 || cid <= 0 || cid > 0xFFFFFFE) {
			return "";
		}
		if (mcc == 460 && (mnc == 0 || mnc == 2 || mnc == 7) && lac > 0
				&& lac < 65534) {
			return "460, 0, " + lac + ", " + cid+"\t"+timeContent;
		}
		if (mcc == 460 && mnc == 1 && cid > 0 && lac > 0 && lac < 65534) {
			return "460, 1, " + lac + ", " + cid+"\t"+timeContent;
		}
		return "";

	}

	/*
	 * 命令解析
	 * 
	 */
	private static CommandLine getCommand(String[] args) throws ParseException {
		CommandLineParser commandParse = new BasicParser();
		Options options =new Options();
		options.addOption("i1",true,"input phone data path");
		options.addOption("i2",true,"input phone_keygps data path");
		options.addOption("i3",true,"input gps data path");
		options.addOption("o",true,"output data path");
		CommandLine commandLine = commandParse.parse(options, args);
		return commandLine;
	}
	//遍历目录，查找文件
	public static void visitPath(FileSystem hdfs, String path,List<Path> listPath) throws IOException {
		Path inputPath = new Path(path);
		FileStatus[] listStatus = hdfs.listStatus(inputPath);
		if(listStatus == null) {
			throw new IOException("the path is not correct:"+path);
		}
		for (FileStatus fileStatus : listStatus) {
			if(fileStatus.isDir()) {
				//System.out.println(fileStatus.getPath().getName()+" -dir-");
				visitPath(hdfs,fileStatus.getPath().toString(),listPath);
			}
			else {
				//System.out.println(fileStatus.getPath().getName()+",len: "+fileStatus.getLen());
				//System.out.println("filename: "+fileStatus.getPath().getName());
				//过滤掉_SUCCESS _log
				if(!"_SUCCESS".equals(fileStatus.getPath().getName())) {
					listPath.add(fileStatus.getPath());
				}
			}
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
	
	public static void deleteDocument(FileSystem fileSystem,String path) throws IOException {
		if(fileSystem.exists(new Path(path))) {
			fileSystem.delete(new Path(path), true);
		}
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		List<Path> listPath1 = new ArrayList<Path>();
		List<Path> listPath2 = new ArrayList<Path>();
		List<Path> listPath3 = new ArrayList<Path>();
		Configuration conf = new Configuration();
//		conf.addResource(new Path("/data/xiaoju/hadoop-2.2.0/etc/hadoop/hdfs-site.xml"));
//		conf.addResource(new Path("/data/xiaoju/hadoop-2.2.0/etc/hadoop/core-site.xml"));
		
		/**
		 * 修改，共两处
		 */
		args = new String[8];
		args[0]="-i1";
		args[1]="hdfs://SparkMaster:9000/eclipse/phone"; 
		args[2]="-i2";
		args[3]="hdfs://SparkMaster:9000/eclipse/phone_gps/";
		args[4]="-i3";
		args[5]="hdfs://SparkMaster:9000/eclipse/gps";
		args[6]="-o";
		args[7]="hdfs://SparkMaster:9000/out";
		
		CommandLine commandLine;
		try {
			commandLine = getCommand(args);
			input1 = commandLine.getOptionValue("i1");
			input2 = commandLine.getOptionValue("i2");
			input3 = commandLine.getOptionValue("i3");
			output = commandLine.getOptionValue("o");
		} catch (Exception e) {
			System.err.println("Usage:");
			System.err.println("-i1 input \t phone data path");
			System.err.println("-i2 input \t phone and site data path");
			System.err.println("-i3 input \t gps data path");
			System.err.println("-o output \t output data path(if the path exists,you should delete it!)");
			System.out.println("two Intermediate input result will be generated at(this 2 path will be deleted automatically):");
			System.out.println("\t1./bigdata-dp/anti/gps_tmp");
			System.out.println("\t2./bigdata-dp/anti/gps_tmp2");
		}
		/**
		 * 修改，共两处
		 * */
//		String midResultPath = "hdfs://bigdata-arch-hdp277.bh:8020/bigdata-dp/anti/gps_tmp";
//		String midResultPath2 = "hdfs://bigdata-arch-hdp277.bh:8020/bigdata-dp/anti/gps_tmp2";
		
		String midResultPath = "hdfs://SparkMaster:9000/bigdata-dp/anti/gps_tmp";
		String midResultPath2 = "hdfs://SparkMaster:9000/bigdata-dp/anti/gps_tmp2";
		
		FileSystem fileSystem = FileSystem.get(new URI(input1),conf);
		//如果存在中间结果目录，则删除之
		deleteDocument(fileSystem,midResultPath);
		deleteDocument(fileSystem,midResultPath2);
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
		visitPath(fileSystem,input1,listPath1);
		visitPath(fileSystem,input2,listPath2);
		visitPath(fileSystem,input3,listPath3);
		
		input1 = splitPathWithComma(listPath1); //phone
		input2 = splitPathWithComma(listPath2);	//data
		input3 = splitPathWithComma(listPath3);	//gps
		
//		System.out.println(input1);
//		System.out.println(input2);
//		System.out.println(input3);
		
		Job job1 = new Job(conf,"job1");
		job1.setJarByClass(PhoneGps.class);
		job1.setMapperClass(MyMap1.class);
		job1.setReducerClass(MyReduce1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job1, input1+","+input2);
		FileOutputFormat.setOutputPath(job1, new Path(midResultPath));
		if(job1.waitForCompletion(true)) {
			counter_found=job1.getCounters().findCounter(GPS_Recod,GPS_FOUND);
			counter_notfound=job1.getCounters().findCounter(GPS_Recod,GPS_NOTFOUND);
			counter_sum=job1.getCounters().findCounter(GPS_Recod,GPS_SUM);
			counter_drivernum=job1.getCounters().findCounter(DRIVER_SUM, DRIVER_SUM);
			System.out.println("---------------------job2-------------");
			Job job2 = new Job(conf,"job2");
			job2.setJarByClass(PhoneGps.class);
			job2.setMapperClass(MyMap2.class);
			job2.setReducerClass(MyReduce2.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPaths(job2, midResultPath+","+input3);
			FileOutputFormat.setOutputPath(job2, new Path(midResultPath2));
			
			if(job2.waitForCompletion(true)) {
				System.out.println("---------------------job3-------------");
				counter_match=job2.getCounters().findCounter(GPS_Recod,GPS_Match);
				Job job3 = new Job(conf,"job3");
				job3.setJarByClass(PhoneGps.class);
				job3.setMapperClass(MyMap3.class);
				job3.setReducerClass(MyReduce3.class);
				job3.setMapOutputKeyClass(Text.class);
				job3.setMapOutputValueClass(Text.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				FileInputFormat.addInputPaths(job3, midResultPath2);
				FileOutputFormat.setOutputPath(job3, new Path(output));
				if(job3.waitForCompletion(true)) {
					
					System.out.println("job down!");
					/*
					 * 全局变量不推荐使用
					System.out.println("gps record total num: "+gps_sum);
					System.out.println("gps record found num: "+gps_found);
					System.out.println("gps record not found num: "+gps_notfound);
					System.out.println("gps record penetration is "+((float)gps_found/gps_sum)*100+"%");
					System.out.println("driver upload gps record average num: "+drivernum);
					*/
					
					
					//if(counter_sum!=null && counter_found!=null && counter_match!=null && counter_notfound!=null && counter_drivernum!=null) {
					if(counter_sum!=null) {
						System.out.println("gps record total num: "+counter_sum.getValue());
					}
					if(counter_found!=null) {
						System.out.println("gps record found num: "+counter_found.getValue());
					}
					if(counter_notfound!=null){
						System.out.println("gps record not found num: "+counter_notfound.getValue());
					}
					if(counter_found!=null && counter_sum!=null){
						System.out.println("gps record penetration is "+((float)counter_found.getValue()/counter_sum.getValue())*100+"%");
						
					}
					if(counter_match!=null){
						System.out.println("gps dic match num: "+counter_match.getValue());
					}
					if(counter_match!=null && counter_found!=null) {
						System.out.println("gps match penetration is "+((float)counter_match.getValue()/counter_found.getValue())*100+"%");
						
					}
					if(counter_sum!=null && counter_drivernum!=null) {
						System.out.println("driver upload gps record average num: "+(float)counter_sum.getValue()/counter_drivernum.getValue());
					}
						
					//}
					
					
					
				}
			}
		}
	}
	
	//mapredu1
	
	static class MyMap1 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			
			if(split.length==1) { //扫描到手机文件
				
				counter_drivernum=context.getCounter(DRIVER_SUM, DRIVER_SUM);
				counter_drivernum.increment(1L);
				
				//全局变量不推荐使用drivernum++;
				context.write(value, new Text("1") );
			}
			else { //扫描到手机坐标文件
				
				
				context.write(new Text(split[0]), new Text(split[1]));
			}
			
		}
		
	}
	
	static class MyReduce1 extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			boolean find=false;
			List<String> values_str = new ArrayList<String>();
			for(Text txt:values) {
				values_str.add(txt.toString());
				if("1".equals(txt.toString())) {
					find=true;
				}
				
			}
			if(find) {
				for(String txt:values_str) {
					if(!"1".equals(txt.toString())) {
						String json_txt =jsonToKey(txt);
						counter_sum=context.getCounter(GPS_Recod,GPS_SUM);
						counter_sum.increment(1L);
						
						//全局变量不推荐使用gps_sum++;
						if(!"".equals(json_txt)) {
							
							counter_found=context.getCounter(GPS_Recod,GPS_FOUND);
							counter_found.increment(1L);
							
							//全局变量不推荐使用gps_found++;
							context.write(key, new Text(json_txt));
						}else {
							counter_notfound=context.getCounter(GPS_Recod,GPS_NOTFOUND);
							counter_notfound.increment(1L);
							//全局变量不推荐使用gps_notfound++;
						}
					}
				}
			}
		}
	}
	
	//mapredue2
	static class MyMap2 extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			if(split.length==2) { //扫描的是字典
				context.write(new Text(split[0]), new Text(split[1]));
			}
			else {
				context.write(new Text(split[1]), new Text(split[0]+"\t"+split[2]));
			}
			
		}
		
	}
	
	static class MyReduce2 extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String gps="";
			boolean  find=false,findGPS=false;
			List<String> values_str = new ArrayList<String>();
			for(Text txt:values) {
				if(txt.toString().split("\t").length>=2) {
					values_str.add(txt.toString()); //只保存phone\ttime
					find=true;
				}
				else {
					gps=txt.toString().split("\t")[0]; //找到gps
					findGPS=true;
				}
				
			}
			if(find&&findGPS) {
				for(String txt:values_str) {
					String[] split = txt.toString().split("\t");
					//计数
					counter_match=context.getCounter(GPS_Recod,GPS_Match);
					counter_match.increment(1L);
					//if(split.length>=2) { //是个电话
						context.write(new Text(split[0]), new Text(gps+"\t"+split[1]));
					//}
				}
			}
			
		}
		
	}
	
	static class GPSTime implements Comparable<GPSTime> {
		String gps;
		String time;
		public GPSTime() {
		}
		public GPSTime(String gps, String time) {
			this.gps = gps;
			this.time = time;
		}
		@Override
		public int compareTo(GPSTime o) {
			long time1 = Long.parseLong(time);
			long time2 = Long.parseLong(o.time);
			return (int)(time1-time2);
			//return (int)(time2-time1);
		}
		
	}
	
	//mapredue3
	static class MyMap3 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t"); //0手机号 1gps 2时间
			context.write(new Text(split[0]), new Text(split[1]+"\t"+split[2]));
		}
		
	}
	
	static class MyReduce3 extends Reducer<Text, Text, Text, Text> {


		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			List<GPSTime> gpsTimeList = new ArrayList<PhoneGps.GPSTime>();
			for(Text txt:values){
				String[] gps_time = txt.toString().split("\t");
				GPSTime gpstime = new GPSTime(gps_time[0],gps_time[1]);
				gpsTimeList.add(gpstime);
			}
			Collections.sort(gpsTimeList);
			StringBuffer sbuffer = new StringBuffer();
			for(int i=0;i<gpsTimeList.size()-1;i++) {
				sbuffer.append(gpsTimeList.get(i).gps+"\t"+gpsTimeList.get(i).time+"\t");
			}
			if(gpsTimeList.get(gpsTimeList.size()-1)!=null) {
				sbuffer.append(gpsTimeList.get(gpsTimeList.size()-1).gps+"\t"+gpsTimeList.get(gpsTimeList.size()-1).time);
			}
			context.write(new Text(key), new Text(sbuffer.toString()));
			
		}
		
	}
	
	
	
	
}
