package demo.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 
 * add jar /srv/nbs/0/apps/wguangliang/short_video_etl/recommend_detail/playconcat.jar;
 * create temporary function playconcat as 'demo.hive.udaf.ConcatPlayFields';
 * 
select device_uuid,split(device_uuid,'#')[0],split(device_uuid,'#')[1],playconcat(vid,occurtime,dura,pg,',')
from datacenter.adm_videorec_play_detail_day
where day='20170801'
group by device_uuid,split(device_uuid,'#')[0],split(device_uuid,'#')[1];
 * 
 * @author wguangliang
 *
 */
public class ConcatPlayFields extends UDAF {
  public static class ConcatFieldsEvaluator implements UDAFEvaluator {
    public static class PartialResult {
      String result;
      String delimiter;
    }

    private PartialResult partial;

    public void init() {
      partial = null;
    }

    public boolean iterate(String vid, String occurtime, String dura,
        String pg, String deli) {
      String value = "{\"vid\":\""+vid+"\",\"occurtime\":\""+occurtime+"\",\"dura\":\""+dura+"\",\"pg\":\""+pg+"\"}";
      if (partial == null) {
        partial = new PartialResult();
        partial.result = new String("");
        if (deli == null || deli.equals("")) {
          partial.delimiter = new String(",");
        } else {
          partial.delimiter = new String(deli);
        }

      }
      if (partial.result.length() > 0) {
        partial.result = partial.result.concat(partial.delimiter);
      }

      partial.result = partial.result.concat(value);

      return true;
    }

    public PartialResult terminatePartial() {
      return partial;
    }

    public boolean merge(PartialResult other) {
      if (other == null) {
        return true;
      }
      if (partial == null) {
        partial = new PartialResult();
        partial.result = new String(other.result);
        partial.delimiter = new String(other.delimiter);
      } else {
        if (partial.result.length() > 0) {
          partial.result = partial.result.concat(partial.delimiter);
        }
        partial.result = partial.result.concat(other.result);
      }
      return true;
    }

    public String terminate() {
      return new String("[" + partial.result + "]");
    }
  }
}
