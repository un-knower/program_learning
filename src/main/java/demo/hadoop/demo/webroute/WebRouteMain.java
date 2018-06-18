package demo.hadoop.demo.webroute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//输入文件 
//三个字段：用户，访问页面时间，访问页面
//字段之间用\t分隔
//
//a	2016-04-01 16:10:15	page1
//a	2016-04-01 13:15:25	page2
//b	2016-04-01 13:15:15	page2
//c	2016-04-02 13:20:15	page7
//d	2016-04-01 14:10:02	page4
//b	2016-04-01 15:10:17	page5
//b	2016-04-01 09:40:30	page10
//c	2016-04-01 15:10:16	page1
//a	2016-04-01 06:10:13	page5
//b	2016-04-01 15:11:14	page5
//d	2016-04-02 14:12:16	page8
//d	2016-04-01 12:10:19	page25
//b	2016-04-01 15:20:16	page6
//b	2016-04-01 18:13:46	page7
//c	2016-04-01 10:10:00	page26
//f	2016-04-01 21:08:17	page6
//c	2016-04-02 01:08:17	page32
//
//生成：
//
//a       2016-04-01 06:10:13,page5       2016-04-01 13:15:25,page2       2016-04-01 16:10:15,page1
//b       2016-04-01 09:40:30,page10      2016-04-01 13:15:15,page2       2016-04-01 15:10:17,page5       2016-04-01 15:11:14,page5     2016-04-01 15:20:16,page6        2016-04-01 18:13:46,page7
//c       2016-04-01 10:10:00,page26      2016-04-01 15:10:16,page1       2016-04-02 01:08:17,page32      2016-04-02 13:20:15,page7
//d       2016-04-01 12:10:19,page25      2016-04-01 14:10:02,page4       2016-04-02 14:12:16,page8
//f       2016-04-01 21:08:17,page6
//
// 或者hive
/*
select uid,concat_ws("\t",collect_set(concat_ws(",",time,pageid)))
from
(
select uid,time,pageid,rank() over (partition by uid order by time)
from webroute
)a
group by uid
*/
//
/**
 * 自定义key
 */
class UserKey implements WritableComparable<UserKey> {
	private String userId="";
	private String time="";
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public UserKey(){}
	public UserKey(String userId, String time) {
		this.userId = userId;
		this.time = time;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.userId = in.readUTF();
		this.time = in .readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(userId);
		out.writeUTF(time);
	}
	@Override
	public int compareTo(UserKey o) { //key与key之间的排序： 按时间进行排序
		if (this.userId.equals(o.getUserId())) { // 如果userId相同，比较time
			Date thisDate = null;
			Date otherDate = null;
			try {
				thisDate = sdf.parse(this.time);
				otherDate = sdf.parse(o.getTime());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			//return thisDate.compareTo(otherDate);
			return (int)(thisDate.getTime()-otherDate.getTime());
		} else {// 如果userId不同，按userId排序
			return this.userId.compareTo(o.getUserId());
		}
	}
	 
	@Override
	public int hashCode() { //控制分发
		return this.userId.hashCode();
	}
	
	public String getUserId() {
		return userId;
	}
	 
	public void setUserId(String userId) {
		this.userId = userId;
	}
	 
	public String getTime() {
		return time;
	}
	 
	public void setTime(String time) {
		this.time = time;
	}
	
}
/**
 * 用户自定义value
 * @author qingjian
 *
 */
class UserValue implements Writable {
	private String time;
	private String page;
	
	public UserValue(){}

	public UserValue(String time, String page) {
		this.time = time;
		this.page = page;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.time = in.readUTF();
		this.page = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(time);
		out.writeUTF(page);
	}

	public String getPage() {
		return page;
	}

	public void setPage(String page) {
		this.page = page;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}
	
	
}
/**
 * 分组类
 */
class GroupComparator extends WritableComparator {

	public GroupComparator() {
		super(UserKey.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		UserKey u1 = (UserKey) a;
		UserKey u2 = (UserKey) b;
		return u1.getUserId().compareTo(u2.getUserId());
	}
	
}
/**
 * 排序类
 */
class SortComparator extends WritableComparator {
	
	public SortComparator() {
		super(UserKey.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		UserKey u1 = (UserKey) a;
		UserKey u2 = (UserKey) b;
		if(u1.getUserId().equals(u2.getUserId())) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:dd");
			Date thisDate = null;
			Date otherDate = null;
			try {
				thisDate = sdf.parse(u1.getTime());
				otherDate = sdf.parse(u2.getTime());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			//return thisDate.compareTo(otherDate);
			return (int)(thisDate.getTime()-otherDate.getTime());
		}else{
			return u1.getUserId().compareTo(u2.getUserId());
		}
	}
	
}



class MyMap extends Mapper<LongWritable, Text, UserKey, UserValue> {
	UserKey userKey = new UserKey();
	UserValue userValue = new UserValue();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, UserKey, UserValue>.Context context)
			throws IOException, InterruptedException {
		if(StringUtils.isNotBlank(value.toString())) {
			String[] split = value.toString().split("\t");
			String userId = split[0];
			String time = split[1];
			String page = split[2];
			userKey.setUserId(userId);
			userKey.setTime(time);
			userValue.setTime(time);
			userValue.setPage(page);
			context.write(userKey, userValue);
		}
		
	}
	
}

class MyReduce extends Reducer<UserKey, UserValue, NullWritable, Text> {
	StringBuffer stringBuffer = new StringBuffer();
	Text result = new Text();
	@Override
	protected void reduce(UserKey key, Iterable<UserValue> values,
			Reducer<UserKey, UserValue, NullWritable, Text>.Context context) throws IOException, InterruptedException {
		System.out.println(key.getUserId()+"------------");
		stringBuffer.delete(0, stringBuffer.length()); //清除上次的结果
		for (UserValue userValue : values) {
			
			System.out.println(userValue.getTime()+"\t"+userValue.getPage());
			stringBuffer.append(userValue.getTime()+","+userValue.getPage()+"\t");
			
		}
		if(StringUtils.isNotBlank(stringBuffer.toString())){
			System.out.println(stringBuffer.toString().substring(0,stringBuffer.length()-1));
			result.set(key.getUserId()+"\t"+stringBuffer.toString().substring(0,stringBuffer.length()-1));
			context.write(NullWritable.get(), result);
		}
	}
	
}
public class WebRouteMain {
	private static final String INPUT_PATH = "hdfs://SparkMaster:9000/eclipse/webroute";
	private static final String OUTPUT_PATH = "hdfs://SparkMaster:9000/out";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(INPUT_PATH), conf);
		check(fileSystem);
		
		Job job = new Job(conf, "");
		
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		job.setMapOutputKeyClass(UserKey.class);
		job.setMapOutputValueClass(UserValue.class);
		job.setGroupingComparatorClass(GroupComparator.class);
//		job.setSortComparatorClass(SortComparator.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.waitForCompletion(true);
		
		
		
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
