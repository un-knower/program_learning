package demo.hadoop.join.repartitionjoin_improved;

import demo.hadoop.join.repartitionjoin.impl.OptimizedDataJoinReducerBase;
import demo.hadoop.join.repartitionjoin.impl.OutputValue;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;


public class SampleReduce extends OptimizedDataJoinReducerBase {

	private TextTaggedOutputValue output = new TextTaggedOutputValue();
	private Text textOutput = new Text();

	@Override
	protected OutputValue combine(String key, OutputValue smallValue, OutputValue largeValue) {
		/*
		 * 你正在执行inner join，只要值中包含NULL值就会返回NULL
		 * 这将导致不生产reduce端的输出
		 */
		if (smallValue == null || largeValue == null) {
			return null;
		}
		Object[] values = { smallValue.getData(), largeValue.getData() };
		textOutput.set(StringUtils.join(values, "\t"));//合并同一键的值，并将其作为reduce端的输出
		output.setData(textOutput);
		return output;
	}
}
