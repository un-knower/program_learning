package demo.hadoop.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class ConcatOneFields extends UDAF {
	 public static class ConcatFieldsEvaluator implements UDAFEvaluator{
	  public static class PartialResult{
	   String result;
	   String delimiter;
	  }
	  private PartialResult partial;
	  
	  public void init() {
	   partial = null;
	  }
	  
	  public boolean iterate(String value,String deli){
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
