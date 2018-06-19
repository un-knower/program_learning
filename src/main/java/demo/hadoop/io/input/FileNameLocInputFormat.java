package demo.hadoop.io.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 * 自定义输入格式
 * 定制输入格式（需要重写①输入格式extends FileInputFormat和重写②读入数据拆分方法extends RecordReader）
 * @author qingjian
 * 产生效果是：map读入数据  key是 FileName@LineOffset  value是该行数据
 */
public class FileNameLocInputFormat extends FileInputFormat<Text, Text>{

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileNameLocRecordReader fnrr = new FileNameLocRecordReader(); //TextInputFormat的默认RecordReader是LineRecordReader
		fnrr.initialize(split, context);
		return fnrr;
	}

}
