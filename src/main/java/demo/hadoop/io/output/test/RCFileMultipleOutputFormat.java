package demo.hadoop.io.output.test;

import java.io.IOException;  
import java.text.NumberFormat;
import java.util.HashMap;  
import java.util.Iterator;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.WritableComparable;  
import org.apache.hadoop.io.compress.CompressionCodec;  
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;  
import org.apache.hadoop.mapreduce.RecordWriter;  
import org.apache.hadoop.mapreduce.TaskAttemptContext;  
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.ReflectionUtils;  

@SuppressWarnings("rawtypes")
public abstract class RCFileMultipleOutputFormat<K extends WritableComparable, V extends BytesRefArrayWritable> extends 
	FileOutputFormat<K, V> {  
	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    /**
     * set number of columns into the given configuration.
     * 
     * @param conf
     *          configuration instance which need to set the column number
     * @param columnNum
     *          column number for RCFile's Writer
     * 
     */
	public static void setColumnNumber(Configuration conf, int columnNum) {
		assert columnNum > 0;
		conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNum);
	}

	/**
     * Returns the number of columns set in the conf for writers.
     * 
     * @param conf
     * @return number of columns for RCFile's writer
     */
	public static int getColumnNumber(Configuration conf) {
		return conf.getInt(RCFile.COLUMN_NUMBER_CONF_STR, 0);
	}
	
	private MultiRecordWriter writer = null;  
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException,  
            InterruptedException {  
        if (writer == null) {  
            writer = new MultiRecordWriter(job, getTaskOutputPath(job));  
        }  
        return writer;  
    }  
    private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {  
        Path workPath = null;  
        OutputCommitter committer = super.getOutputCommitter(conf);  
        if (committer instanceof FileOutputCommitter) {  
            workPath = ((FileOutputCommitter) committer).getWorkPath();  
        } else {  
            Path outputPath = super.getOutputPath(conf);  
            if (outputPath == null) {  
                throw new IOException("Undefined job output-path");  
            }  
            workPath = outputPath;  
        }  
        return workPath;  
    }  
  
    protected abstract String generateFileNameForKeyValue(K key, V value, String name);  
    public class MultiRecordWriter extends RecordWriter<K, V> {  
  
        private HashMap<String, RecordWriter<K, V>> recordWriters = null;  
        private TaskAttemptContext job = null;  

        private Path workPath = null;  
        public MultiRecordWriter(TaskAttemptContext job, Path workPath) {  
            super();  
            this.job = job;  
            this.workPath = workPath;  
            recordWriters = new HashMap<String, RecordWriter<K, V>>();  
        }  
        @Override  
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {  
            Iterator<RecordWriter<K, V>> values = this.recordWriters.values().iterator();  
            while (values.hasNext()) {  
                values.next().close(context);  
            }  
            this.recordWriters.clear();  
        }  
        @Override  
        public void write(K key, V value) throws IOException, InterruptedException {  
            //�õ�����ļ��� 
        	TaskID taskId = job.getTaskAttemptID().getTaskID();
            int partition = taskId.getId();
            String baseName = generateFileNameForKeyValue(key, value, NUMBER_FORMAT.format(partition));  
            RecordWriter<K, V> rw = this.recordWriters.get(baseName);  
            if (rw == null) {  
                rw = getBaseRecordWriter(job, baseName);  
                this.recordWriters.put(baseName, rw);  
            }  
            rw.write(key, value);  
        }  
		// ${mapred.out.dir}/_temporary/_${taskid}/${nameWithExtension}  
        private RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job, String baseName)  
                throws IOException, InterruptedException {  
        	Configuration conf = job.getConfiguration();
        	Path path = new Path(workPath, baseName);  
            FileSystem fs = path.getFileSystem(conf);
            CompressionCodec codec = null;
            if (getCompressOutput(job)) {
              Class<?> codecClass = getOutputCompressorClass(job, DefaultCodec.class);
              codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            }
            final RCFile.Writer out = new RCFile.Writer(fs, conf, path, job, codec);

            return new RecordWriter<K, V>() {

              @Override
              public void close(TaskAttemptContext job) throws IOException {
                out.close();
              }

              @Override
              public void write(WritableComparable key, BytesRefArrayWritable value)
                  throws IOException {
                out.append(value);
              }
            };
        } 

    }  
}  
