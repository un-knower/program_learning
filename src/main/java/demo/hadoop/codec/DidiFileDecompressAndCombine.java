package demo.hadoop.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
//http://cr.intra.xiaojukeji.com/D4460
public class DidiFileDecompressAndCombine {
	private static final Path MID_FILE_PATH = new Path("/tmp/wgl/zc_order_merger_result_hour"); 
//	private static final Path MID_FILE_PATH = new Path("hdfs://sparkmaster:9000/tmp/wgl/zc_order_merger_result_hour"); 
	//使用文件拓展名来codec来对文件进行解压缩
	public static void upcompress(String uri,FileSystem fileSystem, Configuration conf) {
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(new Path(uri));
		if(codec == null) {
			System.out.println("no codec found for "+uri);
			return ;
		}
		String outputFilename = CompressionCodecFactory.removeSuffix(new Path(uri).getName(), codec.getDefaultExtension());
		InputStream in = null;
		OutputStream out = null;
		
		try {
			in = codec.createInputStream(fileSystem.open(new Path(uri)));
//			BufferedReader br = new BufferedReader(new InputStreamReader(in));
//			String content = null;
//			while((content = br.readLine())!=null) {
//				System.out.println(content);
//			}
			out = fileSystem.create(new Path(MID_FILE_PATH.toString()+"/"+outputFilename));//创建文件
			IOUtils.copyBytes(in, out, conf);
			
		} catch (IllegalArgumentException e) {
		
			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
		}
		
		
		
	}
	
	public static void main(String[] args) {
//		args = new String[2];
//		args[0] = "hdfs://sparkmaster:9000/eclipse/compress/";
//		args[1] = "2016032313";
		if(args.length != 2) {
			System.err.println("hadoop jar DidiFileDecompressAndCombina.jar 包名.类名 文件路径 年月日时");
			System.err.println("hadoop jar DidiFileDecompressAndCombine.jar codec.DidiFileDecompressAndCombine /user/aegis/hive/antispam/zc_order_merger_result/deltaNew/ 2016041111");
			return;
		}
		String inputPath = args[0];
		String hour = args[1];
		Configuration conf = new Configuration();
		String dataPath = inputPath+"/"+hour;
		try {
			FileSystem fileSystem = FileSystem.get(URI.create(dataPath), conf);
			
			//处理中间数据目录
			if(fileSystem.exists(MID_FILE_PATH)) {
				fileSystem.delete(MID_FILE_PATH, true);
			}
			
			fileSystem.mkdirs(MID_FILE_PATH);//创建中间结果文件目录
			
			FileStatus[] listStatus = fileSystem.listStatus(new Path(dataPath));
			
			for(FileStatus fileStatus : listStatus) {
				upcompress(fileStatus.getPath().toString(),fileSystem, conf);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
		
	}
}
