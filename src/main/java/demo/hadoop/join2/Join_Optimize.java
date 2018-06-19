package demo.hadoop.join2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * http://blog.csdn.net/garychenqin/article/details/48248051
 * 在reduce端做join，因为values是Iterable，所以需要用一个list保存遍历过的数据，而List的最大值为Integer.MAX_VALUE,所以 在数据量巨大的时候,会造成List越界的错误.所以对这个实现的优化显得很有必要. 
 * 改进点：
 * 改进的地方就是如果对于某个地址ID的迭代器values,如果values的第一个元素是地址信息的话, 
那么,我们就不需要缓存所有的人员信息了.如果第一个元素是地址信息,我们读出地址信息后,后来就全部是人员信息,那么就可以将人员的地址置为相应的地址. 
 * 
 * map端输出 key的compareTo()方法决定排序顺序，hashCode()方法决定发送到哪个reduce
 * Partitioner使用key的 hashCode来决定将该key输送到哪个reducer;
 * shuffle将每个partitioner输出的结果根据key进行group以及排序,分组决定一个key到底对应什么values 	
 * 所以，如果使用自定义key的话，需要重写分组函数！！！
 *
 * 自定义key可以利用hadoop本身的Shuffle进行排序
 * 然后写MyCompator 进行分组
 */
class UserKey implements WritableComparable<UserKey>{
	private Long keyId;  //关联字段
	private boolean primary; //用于将地址信息排在前面
	
	public UserKey() {
	}
	
	public UserKey(long keyId, boolean primary) {
		this.keyId = keyId;
		this.primary = primary;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		this.keyId = in.readLong();
		this.primary = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(keyId);
		out.writeBoolean(primary);
	}

	@Override
	public int compareTo(UserKey o) {  //同一个reduce分片的数据，key之间的排序
		long minus = this.getKeyId() - o.getKeyId();
		if(minus == 0) {
			if(this.primary == o.isPrimary()) {
				return 0;
			}else {
				return this.primary?1:-1;  //false得到-1，会排序到前面
			}
		}
		else {
			return (int) minus;
		}
	}
	
	
	/*key的hashCode方法会决定数据进入那个分片*/
	@Override
	public int hashCode() {  //该key分配到哪个reducer
		return keyId.hashCode();
	}


	public Long getKeyId() {
		return keyId;
	}

	public void setKeyId(Long keyId) {
		this.keyId = keyId;
	}

	public boolean isPrimary() {
		return primary;
	}

	public void setPrimary(boolean primary) {
		this.primary = primary;
	}
	
	
}

class Member implements Writable {
	private String userNo = "";
	private String userName = "";
	private String cityNo = "";
	private String cityName = "";
	
	public String getUserNo() {
		return userNo;
	}
	public void setUserNo(String userNo) {
		this.userNo = userNo;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getCityNo() {
		return cityNo;
	}
	public void setCityNo(String cityNo) {
		this.cityNo = cityNo;
	}
	public String getCityName() {
		return cityName;
	}
	public void setCityName(String cityName) {
		this.cityName = cityName;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.userNo = in.readUTF();
		this.userName = in.readUTF();
		this.cityNo = in.readUTF();
		this.cityName = in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(userNo);
		out.writeUTF(userName);
		out.writeUTF(cityNo);
		out.writeUTF(cityName);
	}
	
	@Override
	public String toString() {
		return this.userNo+"\t"+this.userName+"\t"+this.cityNo+"\t"+this.cityName;
	}
}

/**
 * 怎么分组，决定一个reduce中的数据怎么分组
 */
class MyCompator implements RawComparator<UserKey> {

	@Override
	public int compare(UserKey o1, UserKey o2) {
		return (int) (o1.getKeyId()-o2.getKeyId());
	}
	/**
	 * @param arg0 表示第一个参与比较的字节数组
	 * @param arg1 表示第一个参与比较的字节数组的起始位置
	 * @param arg2 表示第一个参与比较的字节数组的偏移量
	 * 
	 * @param arg3 表示第二个参与比较的字节数组
	 * @param arg4 表示第二个参与比较的字节数组的起始位置
	 * @param arg5 表示第二个参与比较的字节数组的偏移量
	 */
	//按照字节进行比较。8是long的字节数
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);
	}

	
}

class MyPartition extends Partitioner<UserKey, Member>{

	@Override
	public int getPartition(UserKey key, Member value, int numPartitions) {
		return (key.getKeyId().hashCode() & Integer.MAX_VALUE)%numPartitions;
	}

}

class JoinMap extends Mapper<LongWritable, Text, UserKey, Member> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, UserKey, Member>.Context context)
			throws IOException, InterruptedException {
		if(StringUtils.isNotBlank(value.toString())) {
			String[] split = value.toString().split("\\s+");
			UserKey userKey = new UserKey();
			Member member = new Member();
			if (split.length==3) { //数据来自于emp
				String userNo = split[0];
				String userName = split[1];
				String cityNo = split[2];
				member.setUserNo(userNo);
				member.setUserName(userName);
				member.setCityNo(cityNo);
				userKey.setKeyId(Long.parseLong(cityNo));
				userKey.setPrimary(true); //false会排在前面，true会排序在后面。设置为true排序在后面
			}else {
				String cityNo = split[0];
				String cityName = split[1];
				member.setCityNo(cityNo);
				member.setCityName(cityName);
				userKey.setKeyId(Long.parseLong(cityNo));
				userKey.setPrimary(false);
			}
			context.write(userKey, member);
		}
		
	}
	
}

class JoinReduce extends Reducer<UserKey, Member, NullWritable, Member> {

	@Override
	protected void reduce(UserKey key, Iterable<Member> values,
			Reducer<UserKey, Member, NullWritable, Member>.Context context)
			throws IOException, InterruptedException {
		System.out.println(key.getKeyId());
		for (Member member : values) {
			System.out.println(member);
		}
		System.out.println("**************");
//1
//		1	北京
//3	王五	1	
//1	张三	1	
//**************
//2
//		2	上海
//2	李四	2	
//**************
//3
//		3	广州
//5	马七	3	
//4	赵六 	3
		Iterator<Member> iterator = values.iterator();
		String cityName = "";
		Member m = null;
		if(iterator.hasNext()) {
			m = iterator.next();  //第一个数据为地址信息
			cityName = m.getCityName();
		}
		if (!"".equals(cityName)) {
			while(iterator.hasNext()) {
				m = iterator.next();
				m.setCityName(cityName);
				context.write(NullWritable.get(), m);
				
			}
		}
		
		
	}
	
	
}



public class Join_Optimize {
	private final static String INPUTPATH = "hdfs://sparkmaster:9000/eclipse/join2";
	private final static String OUTPUTPATH = "hdfs://sparkmaster:9000/out";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(INPUTPATH), conf);
		check(fileSystem);
		
		Job job = new Job(conf, "join");
		job.setJarByClass(Join_Optimize.class);
		job.setMapperClass(JoinMap.class);
		job.setReducerClass(JoinReduce.class);
		
		job.setMapOutputKeyClass(UserKey.class);
		job.setMapOutputValueClass(Member.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Member.class);
//		job.setNumReduceTasks(4);
		job.setGroupingComparatorClass(MyCompator.class);
//		job.setPartitionerClass(MyPartition.class); //分区part
		FileInputFormat.addInputPath(job, new Path(INPUTPATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUTPATH));
		
		System.exit(job.waitForCompletion(true)?0:-1);
		
	}
	
	public static void check(FileSystem fileSystem) throws IOException {
        if(fileSystem.exists(new Path(OUTPUTPATH))) {
            fileSystem.delete(new Path(OUTPUTPATH), true);
        }
        if(!fileSystem.exists(new Path(INPUTPATH))) {
            System.err.println("Usage: Data Source not Found");
            System.exit(1);
        }
	}
	
}





//LongWritable的源码 自定义了hashCode，compareTo和内置分组比较器

//public class LongWritable implements WritableComparable<LongWritable> {
//private long value;
//
//public LongWritable() {}
//
//public LongWritable(long value) { set(value); }
//
///** Set the value of this LongWritable. */
//public void set(long value) { this.value = value; }
//
///** Return the value of this LongWritable. */
//public long get() { return value; }
//
//@Override
//public void readFields(DataInput in) throws IOException {
//  value = in.readLong();
//}
//
//@Override
//public void write(DataOutput out) throws IOException {
//  out.writeLong(value);
//}
//
///** Returns true iff <code>o</code> is a LongWritable with the same value. */
//@Override
//public boolean equals(Object o) {
//  if (!(o instanceof LongWritable))
//    return false;
//  LongWritable other = (LongWritable)o;
//  return this.value == other.value;
//}
//
//@Override
//public int hashCode() {
//  return (int)value;
//}
//
///** Compares two LongWritables. */
//@Override
//public int compareTo(LongWritable o) {
//  long thisValue = this.value;
//  long thatValue = o.value;
//  return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
//}
//
//@Override
//public String toString() {
//  return Long.toString(value);
//}
//
///** A Comparator optimized for LongWritable. */ 
//public static class Comparator extends WritableComparator {
//  public Comparator() {
//    super(LongWritable.class);
//  }
//
//  @Override
//  public int compare(byte[] b1, int s1, int l1,
//                     byte[] b2, int s2, int l2) {
//    long thisValue = readLong(b1, s1);
//    long thatValue = readLong(b2, s2);
//    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
//  }
//}
//
///** A decreasing Comparator optimized for LongWritable. */ 
//public static class DecreasingComparator extends Comparator {
//  
//  @Override
//  public int compare(WritableComparable a, WritableComparable b) {
//    return -super.compare(a, b);
//  }
//  @Override
//  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//    return -super.compare(b1, s1, l1, b2, s2, l2);
//  }
//}
//
//static {                                       // register default comparator
//  WritableComparator.define(LongWritable.class, new Comparator());
//}
//
//}
