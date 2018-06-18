package demo.hadoop.hbase.syncbatch;



import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncHdfsToHbase {
	private static CommandLine getCommandLine(String[] args) throws ParseException{
		// 解析命令行
		CommandLineParser parser = new BasicParser();
		Options options = new Options();
		options.addOption("i", "input", true,"the directory or file to read from HDFS");
		options.addOption("t", "table", true, "table to import into");
		options.addOption("c", "column", true, "colum to store row data into");
		CommandLine commandLine = parser.parse(options, args);
		return commandLine;

	}

	public static void main(String[] args) throws ParseException, IOException, URISyntaxException {
//		args = new String[6];
//		args[0]="-i";
//		args[1]="hdfs://nn1.heracles.sohuno.com:8020/user/video-mvc/hive/warehouse/personal_push/push_recommender/all_recommender";
//		// args[1]="hdfs://localhost:9000/eclipse/shortModelInfo";
//		// args[1]="hdfs://localhost:9000/eclipse/pushVideo_uncompress";
//		args[2]="-t";
//		args[3]="hbase_t_m_user_shortmodel_pushvideo";
//		// args[3]="hbase_t_m_user_shortmodel_pushvideo";
//		args[4]="-c";
//		args[5]="push:vid";
		// args[5]="push:vid,pushpast:vid";
		final Logger LOGGER = LoggerFactory.getLogger(SyncHdfsToHbase.class);
		
		CommandLine commandLine = getCommandLine(args);
		String INPUT_PATH = commandLine.getOptionValue("i");
		String TABLE_NAME = commandLine.getOptionValue("t");
		String COLUMN = commandLine.getOptionValue("c"); //family:colume
		long start = System.currentTimeMillis();
		run(INPUT_PATH,TABLE_NAME,COLUMN);
		long end = System.currentTimeMillis();
		System.out.println("job down,用时："+(end-start));
	}
	public static void run(String inputPath,String tableName,String column) throws IOException, URISyntaxException {
		Queue<List<Put>> dataQueue = new ConcurrentLinkedQueue<List<Put>>();
		Thread threadInserThread = new Thread(new SyncBatchImportHbase(dataQueue, tableName,column),"InsertHbase");
		threadInserThread.start();
		Thread threadReadFile = new Thread(new SyncReadHdfs(dataQueue,threadInserThread,inputPath,column),"readHdfsFile");
		threadReadFile.start();
	}
}
