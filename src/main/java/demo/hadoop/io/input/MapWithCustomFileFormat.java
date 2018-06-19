package demo.hadoop.io.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapWithCustomFileFormat extends Mapper<Text, Text, Text, Text>{

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//InputFormat:FileNameLocInputFormat
		//RecordReader:FileNameLocRecordReader
		//key:filename@lineoffset
		//value:line string
		
		System.out.println("key = "+key.toString());
		System.out.println("value = "+value.toString());
	}
	
}
