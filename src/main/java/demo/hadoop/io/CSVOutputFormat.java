package demo.hadoop.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
//														输出key类型		输出value类型
public class CSVOutputFormat extends FileOutputFormat<TextArrayWritable, NullWritable> {
	public static String CSV_TOKEN_SEPARATOR_CONFIG = "csvoutputformat.token.delimiter";

	@Override
	public RecordWriter<TextArrayWritable, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		//Configuration conf = HadoopCompat.getConfiguration(job); // 反射生成Configuration，用于读取configuration配置
		boolean isCompress = getCompressOutput(job); // 是否输出压缩
														// mapreduce.output.fileoutputformat.compress
		String keyValueSeparator = conf.get(CSV_TOKEN_SEPARATOR_CONFIG, ","); //从配置中读取分隔符，如果不存在则取","
		CompressionCodec codec = null;
		String extension = ""; // 文件后缀
		if (isCompress) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
			codec = ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension); // 文件生成路径
		FileSystem fileSystem = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fileSystem.create(file, false);
		if (!isCompress) { // 如果未压缩
			return new CSVRecorderWriter(fileOut, keyValueSeparator);
		} else {
			CompressionOutputStream compressionOutputStream = codec.createOutputStream(fileOut);
			return new CSVRecorderWriter(new DataOutputStream(compressionOutputStream), keyValueSeparator);
		}

	}

	protected static class CSVRecorderWriter extends RecordWriter<TextArrayWritable, NullWritable> {

		private static final String utf8 = "UTF-8";
		private static final byte[] newline;

		static {
			try {
				newline = "\n".getBytes(utf8);

			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("can't find " + utf8 + " encoding");
			}
		}

		protected DataOutputStream out;
		private final String csvSeparator;

		public CSVRecorderWriter(DataOutputStream out, String csvSeparator) {
			this.out = out;
			this.csvSeparator = csvSeparator;
		}

		@Override
		public void write(TextArrayWritable key, NullWritable value) throws IOException, InterruptedException {
			if (key == null) {
				return;
			}
			boolean first = true;
			for (Writable field : key.get()) {
				writeObject(first, field);
				first = false;
			}
			out.write(newline); //默认加一个换行符
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 *
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
		 */
		private void writeObject(boolean first, Writable o) throws IOException {
			if (!first) { //如果不是第一个
				out.write(csvSeparator.getBytes(utf8)); //写入一个分隔符的bytes
			}

			boolean encloseQuotes = false;
			if (o.toString().contains(csvSeparator)) { //如果字段包含分隔符，则引入引号
				encloseQuotes = true;
			}

			if (encloseQuotes) {
				out.write("\"".getBytes(utf8));
			}
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength()); //写字段
			} else {
				out.write(o.toString().getBytes(utf8));
			}
			if (encloseQuotes) {
				out.write("\"".getBytes(utf8));
			}

		}

		@Override
		public synchronized void close(TaskAttemptContext context) throws IOException, InterruptedException {
			out.close();
			
		}

	}

}
