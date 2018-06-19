package demo.hadoop.cmd;

import java.util.Date;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;



public class CommandLineParseTest {
	public static Options getOptions() {
		
		Options options = new Options();
						 //短参数               长参数       是否需要跟参数                   解释说明           
		options.addOption("i1", "input1", true, "input info path from hadoop, required");
		options.addOption("i2", "input2", true, "input info path from mysql, required");
		options.addOption("o", "output", true, "result output path, required");
		Option filesOption = OptionBuilder.withArgName("args...")  //help打印信息<args...>
								.withLongOpt("files")  //长参数
								.hasArgs()  //需要跟多个参数  //如果只需要跟一个参数 使用 .hasArg()
								.withDescription("input one or more files") //解释说明
								.create("f"); //短参数
		options.addOption(filesOption);
		options.addOption("t", "time", false, "print current time");
		options.addOption("h", "help", false, "get help infomation");
		
		return options;
		
	}
	
	public static CommandLine getCommand(Options options,String[] args) throws ParseException {
		CommandLineParser commandParse = new BasicParser(); //风格 ：   参数 参数值
//		CommandLineParser commandParse = new PosixParser(); //风格：   参数 参数值   //POSIX（Portable Operating System Interface of Unix）中的参数形式，例如 tar -zxvf foo.tar.gz
//		CommandLineParser commandParse = new GnuParser(); //风格： 1 参数 参数值   2 参数=参数值  //GNU 中的长参数形式，例如 du --human-readable --max-depth=1
		
		CommandLine commandLine = commandParse.parse(options, args);
		
		return commandLine;
	}
	
	public static void PrintHelp(Options options) {
		// print usage  
	    HelpFormatter formatter = new HelpFormatter();  
	    formatter.printHelp( "ProjectName Usage", options);  
	    System.exit(0);
	}
	
	public static void main(String[] args) throws ParseException {
		Options options = getOptions();
		CommandLine command = getCommand(options, args);
		
		if(command.hasOption('h')) {
			PrintHelp(options);
		}
		String inputPath1 = null;
		String inputPath2 = null;
		String outputPath = null;
		if(command.hasOption("i1")) {
			inputPath1 = command.getOptionValue("i1");
		}else {
			PrintHelp(options);
		}
		if(command.hasOption("i2")) {
			inputPath2 = command.getOptionValue("i2");
		}else {
			PrintHelp(options);
		}
		if(command.hasOption("o")) {
			outputPath = command.getOptionValue("o");
		}else {
			PrintHelp(options);
		}
		if(command.hasOption("t")) {
			System.out.println(new Date());
		}
		if(command.hasOption("f")) {
			String[] files = command.getOptionValues("f");
			for (String file : files) {
				System.out.println(file);
			}
		}else {
			PrintHelp(options);
		}
		
		System.out.println("inputPath1:"+inputPath1);
		System.out.println("inputPath2:"+inputPath2);
		System.out.println("outputPath:"+outputPath);
		
	}
	
}
