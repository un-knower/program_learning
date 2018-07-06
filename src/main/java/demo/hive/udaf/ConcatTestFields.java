package demo.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
/**
 * 
 * 
CREATE TABLE `temp.test`(
  `id` string, 
  `uid` string, 
  `addr` string, 
  `phone` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://hz-cluster3/user/datacenter/warehouse/temp.db/test'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'totalSize'='88', 
  'transient_lastDdlTime'='1501643310')
  
add jar /srv/nbs/0/apps/wguangliang/short_video_etl/recommend_detail/testconcat.jar;
create temporary function helloconcat as 'demo.hive.udaf.ConcatTestFields';

select u,helloconcat(addr,phone,',')
from 
(
select id,u,addr,phone from temp.test lateral view explode(split(uid, ',')) t as u
)a
group by u;

 *
 */
public class ConcatTestFields extends UDAF {
	 public static class ConcatFieldsEvaluator implements UDAFEvaluator{
	  public static class PartialResult{
	   String result;
	   String delimiter;
	  }
	  private PartialResult partial;
	  
	  public void init() {
	   partial = null;
	  }
	  
	  public boolean iterate(String addr,String phone,String deli){
	   String value= "{\"addr\":\""+addr+"\",\"phone\":"+phone+"}";
	   if (partial == null){
	    partial = new PartialResult();
	    partial.result = new String("");
	    if(  deli == null || deli.equals("") )
	    {
	     partial.delimiter = new String(",");
	    }
	    else
	    {
	     partial.delimiter = new String(deli);
	    }
	        
	   }
	   if ( partial.result.length() > 0 )
	   {
	    partial.result = partial.result.concat(partial.delimiter);
	   }
	   
	   partial.result = partial.result.concat(value);
	   
	   return true;
	  }
	  
	  public PartialResult terminatePartial(){
	   return partial;
	  }
	  
	  public boolean merge(PartialResult other){
	   if (other == null){
	    return true;
	   }
	   if (partial == null){
	    partial = new PartialResult();
	    partial.result = new String(other.result);
	    partial.delimiter = new String(other.delimiter);
	   }
	   else
	   {   
	    if ( partial.result.length() > 0 )
	    {
	     partial.result = partial.result.concat(partial.delimiter);
	    }
	    partial.result = partial.result.concat(other.result);
	   }
	   return true;
	  }
	  
	  public String terminate(){
	   return new String("["+partial.result+"]");
	  }
	 }
	}
