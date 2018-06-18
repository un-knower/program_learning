package demo.hadoop.hdfs;

import java.io.InputStream;
import java.net.URL;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;
/**
 * 读取hdfs上的文件内容
 * @author qingjian
 *
 */
public class CatHdfs {
	//hadoop fs -ls hdfs://localhost:9000/test
	final static String PATH="hdfs://localhost:9000/eclipse/build.xml";
	
	public static void main(String[] args) throws Exception {
		//使URL能够解析hdfs
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		final URL url = new URL(PATH);
		final InputStream in = url.openStream();
		/**
		 * @param in	表示输入流
		 * @param out	表示输出流
		 * @param bufferSize 表示缓冲大小
		 * @param close	表示是否自动关闭流
		 */
		IOUtils.copyBytes(in, System.out, 1204, true);
	}
}
