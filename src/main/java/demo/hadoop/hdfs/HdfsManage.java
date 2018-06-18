package demo.hadoop.hdfs;

import java.io.FileInputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/*
 * HDFS端用的是FSDataOutputStream和FSDataInputStream
 * 
 * HDFS的各种操作
 * 
 */
public class HdfsManage {
	//得到FileSystem
	public static FileSystem getFileSystem() throws Exception{
		//FileSystem fileSystem = FileSystem.get(URI.create(PATH), new Configuration());
		return FileSystem.get(new URI(PATH), new Configuration(),"hadoop");//最后为用户名。因为该系统默认用户为qingjian，而该用户在hadoop中权限很小，所以需要切换为hadoop用户
	}
	static final String PATH = "hdfs://localhost:9000/eclipse/build.xml";
	static final String DIR = "hdfs://localhost:9000/eclipse";
	static final String FILE = "hdfs://localhost:9000/eclipse/HdfsTest1.java";
	//static final String DIR = "/test"; //也行。因为getFileSystem已经给予全路径了
	public static void main(String[] args) throws Exception {
		FileSystem fileSystem = getFileSystem();
		//在HDFS创建文件夹 hadoop fs -mkdir
		fileSystem.mkdirs(new Path(DIR)); //返回boolean
		
		//在HDFS创建文件
		FSDataOutputStream out2 = fileSystem.create(new Path("/test.txt")); //创建文件
		byte[] buff = "hello hadoop world!\n".getBytes();
		 /**
	     * Writes <code>len</code> bytes from the specified byte array
	     * starting at offset <code>off</code> to the underlying output stream.
	     * If no exception is thrown, the counter <code>written</code> is
	     * incremented by <code>len</code>.
	     *
	     * @param      b     the data.数据
	     * @param      off   the start offset in the data.开始
	     * @param      len   the number of bytes to write.传输数据长度
	     */
		out2.write(buff, 0, buff.length);
	
		
		//重命名HDFS文件
		//设置旧的文件名
		Path frpath = new Path("/test.txt");
		Path topath = new Path("/test1.txt");
		//重命名
		boolean isRename = fileSystem.rename(frpath, topath);
		String result = isRename?"成功":"失败";
		System.out.println("文件重命名结果为： "+result);
		
		//上传文件 hadoop fs -put src des
		FSDataOutputStream out = fileSystem.create(new Path(FILE));
		FileInputStream in = new FileInputStream("/home/qingjian/workspace/HadoopDemo/src/hdfs/HdfsTest1.java");
		IOUtils.copyBytes(in, out, 1024, true);
		
		//下载文件 hadoop fs -get src des
		/*
		FSDataInputStream in2 = fileSystem.open(new Path(FILE), 1024);
		IOUtils.copyBytes(in2, System.out, 1024,true);
		*/
		//浏览文件夹 hadoop fs -ls 路径
		FileStatus[] listStatus = fileSystem.listStatus(new Path(DIR));//不是递归显示
		for(FileStatus fileStats:listStatus) {
			System.out.println(fileStats.isDir()?"文件夹":"文件");
			System.out.println("权限信息： "+fileStats.getPermission());
			System.out.println("副本数："+fileStats.getReplication());
			System.out.println("路径信息： "+fileStats.getPath());
		}
		//删除文件夹
		/**
		 * @param 路径
		 * @param recursive 如果该路径为目录，则设置为true，这个目录就会被删除。否则的话抛出异常
		 * 					如果该路径为文件，则设置为true或者false都行
		 */
		//fileSystem.delete(new Path(DIR), true);
		
		
		//判断某个HDFS文件是否存在
		Path findf = new Path("/test1.txt");
		boolean isExists = fileSystem.exists(findf);
		
	}
}
