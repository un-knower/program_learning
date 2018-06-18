package demo.hadoop.codec;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class FileDecompressor {
	private static final String FILE_PATH="hdfs://localhost:9000/eclipse/pushVideo/000000_0.lzo_deflate";
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//
		String name = "io.compression.codecs";  
	    String value = "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.BZip2Codec"; 
	//	String value = "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec";  
	    conf.set(name, value);  
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("dfs.socket.timeout", "180000");//将该值改大，防止hbase超时退出
		
		FileSystem fs = FileSystem.get(URI.create(FILE_PATH), conf);
		Path inputPath = new Path(FILE_PATH);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if(codec == null) {
			System.out.println("No codec found for "+FILE_PATH);
		}
		//去掉后缀名
		String outputUri = CompressionCodecFactory.removeSuffix(FILE_PATH, codec.getDefaultExtension());
		//System.out.println(codec.getDefaultExtension());
		InputStream in = null;
		OutputStream out = null;
		try{
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			System.out.println(in);//org.apache.hadoop.fs.FSDataOutputStream@1907bac//保存在了当前hdfs目录下
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String readLineTemp="";
			while((readLineTemp=br.readLine()) != null) {
				System.out.println(readLineTemp);
			}
			//IOUtils.copyBytes(in, System.out, conf);  //将读取内容打印到控制台//
			IOUtils.copyBytes(in, out, conf); //将读取内容输出到out里
			
		}catch(Exception e) {
			
		}finally{
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
}
