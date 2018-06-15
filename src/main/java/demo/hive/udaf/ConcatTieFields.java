package demo.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 
add jar /srv/nbs/0/apps/wangguangliang/short_video_etl/recommend_detail/tieconcat.jar;
create temporary function tieconcat as 'demo.hive.udaf.ConcatTieFields';
 * 
select device_uuid,split(device_uuid,'#')[0],split(device_uuid,'#')[1],tieconcat(ip,vid,title,occurtime,content,',')
from datacenter.adm_videorec_play_detail_day
where day='20170801'
group by device_uuid,split(device_uuid,'#')[0],split(device_uuid,'#')[1];
 * 
 * @author bjwangguangliang
 *
 */
public class ConcatTieFields extends UDAF {
  public static class ConcatFieldsEvaluator implements UDAFEvaluator {
    public static class PartialResult {
      String result;
      String delimiter;
    }

    private PartialResult partial;

    public void init() {
      partial = null;
    }

    public boolean iterate(String ip, String vid, String title, String occurtime, String content, String deli) {
      String value = "{\"ip\":\""+ip+"\",\"vid\":\""+vid+"\",\"title\":\""+new String(title)+"\",\"occurtime\":\""+occurtime+"\",\"content\":\""+content+"\"}";
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
