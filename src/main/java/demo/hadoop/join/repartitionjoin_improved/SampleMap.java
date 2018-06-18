package demo.hadoop.join.repartitionjoin_improved;

import demo.hadoop.join.repartitionjoin.impl.OptimizedDataJoinMapperBase;
import demo.hadoop.join.repartitionjoin.impl.OutputValue;

import org.apache.hadoop.io.Text;


public class SampleMap extends OptimizedDataJoinMapperBase {

  private boolean smaller;

  /*
   * 这个方法需要对应每个输入文件并返回一个独立的标识符，所以需要返回文件名
   */
  @Override
  protected Text generateInputTag(String inputFile) {
    // tag the row with input file name (data source)
    smaller = inputFile.contains("users.txt");//设定users.txt这个文件是较小的文件
    return new Text(inputFile);
  }
  /*
   * 这个MapReduce作业使用KeyValueTextInputFormat
   * 所以键包含用户名，且这个用户名是合并字段
   */
  @Override
  protected String genGroupKey(Object key, OutputValue output) {
    return key.toString();
  }

  /*
   * 表明这个input split是否来自较小的文件
   */
  @Override
  protected boolean isInputSmaller(String inputFile) {
    return smaller;
  }

  /*
   * 生成发送给reduce端的输出。
   * 另外，由于这个作业将使用KeyValueTextInputFormat
   * 那么值包含用户信息，且这个值需要返回给回调者
   */
  @Override
  protected OutputValue genMapOutputValue(
      Object o) {
    return new TextTaggedOutputValue((Text) o);
  }
}
