package demo.hadoop.io;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import demo.hadoop.com.cloudera.datascience.common.XmlInputFormat;

/**
 * Hadoop读取XML文件
 * @author qingjian
 *
 */
public class XMLRead {
	static class Map extends Mapper<LongWritable, Text, Text, Text> {
		Text resultKey = new Text();
		Text resultValue = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String document = value.toString();
//			System.out.println("'"+document+"'");
			try {
				XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(new ByteArrayInputStream(document.getBytes()));
				String propertyName = "";
				String propertyValue = "";
				String currentElement = "";
				while(reader.hasNext()) {
					int code = reader.next();
					switch(code) {
					case XMLStreamConstants.START_ELEMENT:
						currentElement = reader.getLocalName();
//						System.out.println(currentElement);
						break;
					
					case XMLStreamConstants.CHARACTERS:
						if(currentElement.equalsIgnoreCase("name")) {
//							System.out.println("->"+reader.getText());
							propertyName += reader.getText().trim()+"\t";
						} else if(currentElement.equalsIgnoreCase("value")) {
							propertyValue += reader.getText().trim()+"\t";
						}
						break;
					}	
				}
				reader.close();
				resultKey.set(propertyName.trim());
				resultValue.set(propertyValue.trim());
				context.write(resultKey, resultValue);
			} catch (XMLStreamException e) {
				e.printStackTrace();
			} catch (FactoryConfigurationError e) {
				e.printStackTrace();
			}
			

		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String INPUT = "hdfs://master:9000/data/xml";
		String OUTPUT = "hdfs://master:9000/output";
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", " ");
		conf.set("xmlinput.start", "<property>");
		conf.set("xmlinput.end", "</property>");
		
		Job job = new Job(conf);
		job.setJarByClass(XMLRead.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path(INPUT));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
		
		job.waitForCompletion(true);
		
		
		
		
		
		
		
		
		
	}
}
