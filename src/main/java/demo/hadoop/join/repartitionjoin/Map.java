package demo.hadoop.join.repartitionjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;

public class Map extends DataJoinMapperBase {


  protected Text generateInputTag(String inputFile) {
    // tag the row with input file name (data source)
    return new Text(inputFile);
  }

  protected Text generateGroupKey(TaggedMapOutput output) {
    // first column in the input tab separated files becomes the key (to perform the JOIN)
    String line = (output.getData()).toString();
    String[] tokens = StringUtils.split(line, "\t", 2);
    String groupKey = tokens[0];
    return new Text(groupKey);
  }

  protected TaggedMapOutput generateTaggedMapOutput(Object value) {
    TaggedMapOutput output = new TextTaggedMapOutput((Text) value);
    output.setTag(new Text(this.inputTag));
    return output;
  }
}
