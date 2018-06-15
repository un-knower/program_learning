package demo.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 
 * add jar /srv/nbs/0/apps/wangguangliang/short_video_etl/recommend_detail/recommendconcat.jar;
 * create temporary function recommendconcat as 'demo.hive.udaf.ConcatRecommendFields2';
 * 
select device_uuid,split(device_uuid,'#')[0],split(device_uuid,'#')[1],recommendconcat(vid,is_rcc,is_share,is_collect,is_uninterested,is_tie,',')
from datacenter.adm_videorec_detail_day
where day='20170801'
group by device_uuid,split(device_uuid,'#')[0],split(device_uuid,'#')[1];
 * 
 * @author bjwangguangliang
 *
 */
public class ConcatRecommendFields2 extends UDAF {
  public static class ConcatFieldsEvaluator implements UDAFEvaluator {
    public static class PartialResult {
      String result;
      String delimiter;
    }

    private PartialResult partial;

    public void init() {
      partial = null;
    }

    public boolean iterate(String vid, String is_rcc, String is_share,
        String is_collect, String is_uninterested, String is_tie, String ev_time, String deli) {
      String value = "{\"vid\":\"" + vid + "\",\"is_rcc\":" + is_rcc + ",\"is_share\":" + is_share + ",\"is_collect\":"
          + is_collect + ",\"is_uninterested\":" + is_uninterested + ",\"is_tie\":" + is_tie + ",\"ev_time\":\"" +ev_time+"\"}";
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
