package demo.hadoop.cmd;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CommandParserTest {
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
	public static void main(String[] args) throws ParseException {
		args = new String[4];
		args[0]="-i1";
		args[1]="i1 value";
		args[2]="-i2";
		args[3]="i2 value";
		//args[4]="-o";
		//args[5]="o value";
		
		CommandLine command = getCommand(args);
		String optionValue = command.getOptionValue("i1");
		System.out.println(optionValue);
		
		
		
		System.out.println(command.toString());
	}
	
}
