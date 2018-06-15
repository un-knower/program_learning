package demo.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 
add jar /srv/nbs/0/apps/wangguangliang/short_video_etl/recommend_detail/interactionconcat.jar;
create temporary function interactionconcat as 'demo.hive.udaf.ConcatInteractionFields';
 * 
select device_uuid,split(device_uuid,'#')[0],split(device_uuid,'#')[1],type,interactionconcat(vid,title,occurtime,',')
from datacenter.adm_videorec_play_detail_day
where day='20170801'
group by device_uuid,split(device_uuid,'#')[0],split(device_uuid,'#')[1],type;
 * 
 * @author bjwangguangliang
 *
 */
public class ConcatInteractionFields extends UDAF {
  public static class ConcatFieldsEvaluator implements UDAFEvaluator {
    public static class PartialResult {
      String result;
      String delimiter;
    }

    private PartialResult partial;

    public void init() {
      partial = null;
    }

    public boolean iterate(String vid, String title, String occurtime, String deli) {
      if (title == null) {
        title = "";
      } else {
        title = title.trim();
      }
      String value = "{\"vid\":\""+vid+"\",\"title\":\""+title+"\",\"occurtime\":\""+occurtime+"\"}";
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
