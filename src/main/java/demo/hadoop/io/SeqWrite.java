package demo.hadoop.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * SequenceFile
 * 是hadoop用来保存二进制形式的<key,value>对而设计的一种平面文件(Flat File)
 * SequenceFile主要是由一个Header后跟多条Record组成
 * 	Header主要包含了Key classname,Value classname存储压缩算法
 * 	Record以键值对的方式进行存储，并且Value值的结构取决于该记录是否被压缩
 * 
 * SequenceFile支持两种哦功能格式的数据压缩，分别是：
 * 	Record compression 是对每条记录的Value进行压缩
 * 	Block compression 是将一连串的Record组织到一起，统一压缩成一个Block
 * @author qingjian
 *
 */
public class SeqWrite {
	private static final String[] data={
		"a,b,c,d,e,f,g",
		"h,i,j,k,l,m,n",
		"o,p,q,r,s,t",
		"u,v,w,x,y,z",
		"0,1,2,3,4",
		"5,6,7,8,9",
	};
	private static final String OUT_PATH="hdfs://localhost:9000/eclipse/seqWrite";
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(OUT_PATH), conf);
		//打开SequenceFile.Writer对象，写入键值
		SequenceFile.Writer writer = null;
		//序列化文件名字
		//Path path = new Path("seqWrite");
		//key
		IntWritable key = new IntWritable();
		//value
		Text value  = new Text();
		try {
			writer=SequenceFile.createWriter(fileSystem, conf, new Path(OUT_PATH), key.getClass(), value.getClass());
			for(int i=0;i<10000;i++) {
				key.set(i);
				value.set(SeqWrite.data[i%SeqWrite.data.length]);
				writer.append(key, value);//调用append方法将内容顺序写入
			}
		}catch(Exception e) {
			
		}finally {
			System.out.println("data put success");
			IOUtils.closeStream(writer);
		}
		
		
	}
}
