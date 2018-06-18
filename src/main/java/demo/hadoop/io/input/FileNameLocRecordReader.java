package demo.hadoop.io.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
/**
 * RecordReader
 * 主要重写方法有：
 * 一、initialize
 * 二、getCurrentKey
 * 三、getCurrentValue
 */
/**
 * 定制输入格式（需要重写①输入格式extends FileInputFormat和重写②读入数据拆分方法extends RecordReader）
 * 
 * @author qingjian
 * 产生效果是：map读入数据  key是 FileName@LineOffset  value是该行数据
 */
public class FileNameLocRecordReader extends RecordReader<Text, Text>{

	String fileName;
	LineRecordReader lrr = new LineRecordReader(); //TextInputFormat的默认RecordReader是LineRecordReader
	/**
	 * 做初始化工作
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		lrr.initialize(split, context);
		fileName = ((FileSplit)split).getPath().getName(); //得到文件名
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return lrr.nextKeyValue();
	}

	/**
	 * LineRecordReader的getCurrentKey方法得到的是偏移量
	 */
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return new Text("("+fileName+"@"+lrr.getCurrentKey()+")");
	}
	/**
	 * LineRecordReader的getCurrentValue方法得到的是该行文本内容
	 */
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return lrr.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lrr.getProgress();
	}

	@Override
	public void close() throws IOException {
		lrr.close();
	}

}
