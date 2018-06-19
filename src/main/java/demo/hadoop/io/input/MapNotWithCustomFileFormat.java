package demo.hadoop.io.input;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapNotWithCustomFileFormat extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//InputFormat:TextInputFormat
		//RecordReader:LineRecordReader
		//key:lineoffset
		//value:line string
		
		System.out.println("key = "+key.toString());
		System.out.println("value = "+value.toString());
	}

	
}
