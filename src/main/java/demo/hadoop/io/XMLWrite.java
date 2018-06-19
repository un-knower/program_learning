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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import demo.hadoop.com.cloudera.datascience.common.XmlInputFormat;

/**
 * Hadoop将读取的XML文件再写入XML文件
 * 需要注意的是
 * 一个名称的标签不能同为一个子标签
 * 例如，不能有如下标签
 * <root>
 * 	<name></name>
 * 	<name></name>
 * 	...
 * </root>
 * 
 * @author qingjian
 *
 */
public class XMLWrite {
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
						break;
					
					case XMLStreamConstants.CHARACTERS:
						if(currentElement.equalsIgnoreCase("name")) {
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
	
	static class Reduce extends Reducer<Text, Text, Text, Text> {

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(new Text("<configuration>"), null);
		}

		private Text outputKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				outputKey.set(constructPropertyXml(key, value));
				context.write(outputKey, null);
			}
		}

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			context.write(new Text("</configuration>"), null);
		}
		
		public static String constructPropertyXml(Text name, Text value) {
			StringBuilder sb = new StringBuilder();
			sb.append("<property><name>").append(name)
			.append("</name><value>").append(value)
			.append("</value></property>");
			return sb.toString();
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
		job.setJarByClass(XMLWrite.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(INPUT));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
		
		job.waitForCompletion(true);
		
		
		
		
		
		
		
		
		
	}
}
