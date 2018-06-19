package demo.hadoop.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class ConcatFields extends UDAF {
	 public static class ConcatFieldsEvaluator implements UDAFEvaluator{
	  public static class PartialResult{
	   String result;
	   String delimiter;
	  }
	  private PartialResult partial;
	  
	  public void init() {
	   partial = null;
	  }
	  
	  public boolean iterate(String vid,String vname,String time,String play_time,String timelength,String deli){
	   String value=vid+" "+vname+" "+time+" "+play_time+" "+timelength;
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
	   return new String(partial.result);
	  }
	 }
	}
