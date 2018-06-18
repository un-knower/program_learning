package demo.hadoop.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for CSV
 * plain text files.  Keys are byte offsets in
 * the file, and values are {@link org.apache.hadoop.io.ArrayWritable}'s with tokenized
 * values.
 */
//														输出key类型 	value类型
public class CSVInputFormat extends FileInputFormat<LongWritable, TextArrayWritable> {
	public static String CSV_TOKEN_SEPARATOR_CONFIG =
		      "csvinputformat.token.delimiter";
	@Override
	public RecordReader<LongWritable, TextArrayWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		String csvDelimiter = context.getConfiguration().get(CSV_TOKEN_SEPARATOR_CONFIG);
		Character separator = null;
		if(csvDelimiter!=null && csvDelimiter.length()==1) {
			separator = csvDelimiter.charAt(0);
		}
		return new CSVRecordReader(separator);
		
	}
	
	public static class CSVRecordReader   //RecordReader类负责从输入文件中读取记录，它将文件中的偏移量为键，将CSV行中的标记数组作为值 
		extends RecordReader<LongWritable, TextArrayWritable> {
		private LineRecordReader reader; //TextInputFormat的默认RecordReader是LineRecordReader
		private TextArrayWritable value;
		private final CSVParser parser;
		
		public CSVRecordReader(Character csvDelimiter) {
			this.reader = new LineRecordReader();
			if(csvDelimiter == null) {
				parser = new CSVParser();
			} else {
				parser = new CSVParser(csvDelimiter);
			}
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			reader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(reader.nextKeyValue()) { //使用LineRecordReader读取下一条记录，一旦达到分片结尾，LineRecordReader.nextKeyValue将返回NULL
				loadCSV();
				return true;
			} else {
				value = null;
				return false;
			}
		}
		
		private void loadCSV() throws IOException {
			String line = reader.getCurrentValue().toString();
			String[] tokens = parser.parseLine(line);
			value = new TextArrayWritable(convert(tokens));
		}
		private Text[] convert(String[] s) {
			Text t[] = new Text[s.length];
			for (int i=0;i<t.length;i++) {
				t[i] = new Text(s[i]);
				
			}
			return t;
		}
		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}
		
	}

}
