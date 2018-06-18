package demo.hadoop.hive.udf;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;


/**
 * 
 * 文件：
 * 
people	        time		place
 * 
 * A	2015-3-24 8:00:00	home
 * A	2015-3-24 10:00:00	super market
 * A	2015-3-24 12:00:00	kfc 
 * A	2015-3-24 15:00:00	school
 * A	2015-3-24 20:00:00	home
 * A	2015-3-25 8:00:00	home
 * A	2015-3-25 10:00:00	park
 * A	2015-3-25 12:00:00	home
 * A	2015-3-25 15:30:00	bank
 * A	2015-3-25 19:00:00	home
 * 
 * 通过查询我们项得到如下结果：

people    day    from_time  from_place  to_time   to_place
 * 
 * A	2015-3-24 8:00:00	home	10:00:00	super market
 * A	2015-3-24 10:00:00	super market	12:00:00	kfc
 * A	2015-3-24 12:00:00	kfc	15:00:00	scholl
 * A	2015-3-24 15:00:00	school	20:00:00	home
 * A	2015-3-25 8:00:00	home	10:00:00	park
 * A	2015-3-25 10:00:00	park	12:00:00	home	
 * A	2015-3-25 12:00:00	home	15:30:00	bank
 * A	2015-3-25 15:30:00	bank	19:00:00	home

 * @author qingjian
 *
 *使用
 *hive> create table whereme(people string,time string,place string)
 *	  > row format delimited
 *	  > fields terminated by '\t'
 *hive> create table tmpResult(info struct<people:string, day:string, from_time:string, from_place:string, to_time:string, to_place:string>);
 *hive> create table whereresult(people string, day string, from_time string, from_place string, to_time string, to_place string);
 *hive> create table whereresult
    > like tmpResult;
    
 *hive>insert overwrite table tmpResult 
 *	  >select hellogenericudf(whereme.people,whereme.time,whereme.place) from whereme;
 *
 *
hive> select hellogenericudf(people,time,place) from whereme;

{"people":"A","day":"2015-03-24","from_time":"null","from_place":"null","to_time":"08:00:00","to_place":"home"}
{"people":"A","day":"2015-03-24","from_time":"08:00:00","from_place":"home","to_time":"10:00:00","to_place":"super market"}
{"people":"A","day":"2015-03-24","from_time":"10:00:00","from_place":"super market","to_time":"12:00:00","to_place":"kfc "}
{"people":"A","day":"2015-03-24","from_time":"12:00:00","from_place":"kfc ","to_time":"15:00:00","to_place":"school"}
{"people":"A","day":"2015-03-24","from_time":"15:00:00","from_place":"school","to_time":"20:00:00","to_place":"home"}
{"people":"A","day":"2015-03-25","from_time":"null","from_place":"null","to_time":"08:00:00","to_place":"home"}
{"people":"A","day":"2015-03-25","from_time":"08:00:00","from_place":"home","to_time":"10:00:00","to_place":"park"}
{"people":"A","day":"2015-03-25","from_time":"10:00:00","from_place":"park","to_time":"12:00:00","to_place":"home"}
{"people":"A","day":"2015-03-25","from_time":"12:00:00","from_place":"home","to_time":"15:30:00","to_place":"bank"}
{"people":"A","day":"2015-03-25","from_time":"15:30:00","from_place":"bank","to_time":"19:00:00","to_place":"home"}


 *
 *
 *
 *
 */
public class HelloGenericUDF extends GenericUDF{
	//输入变量定义
	private ObjectInspector peopleObj;
	private ObjectInspector timeObj;
	private ObjectInspector placeObj;
	//之前记录保存
	String strPreTime="";
	String strPrePlace="";
	String strPrePeople="";
	//确认输入类型是否正确
	//输出类型定义
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		peopleObj = arguments[0];
		timeObj = arguments[1];
		placeObj = arguments[2];
		//输出结构体定义,即为描述表结构，因为输出表结构是struct类型，所以要封装成struct
		ArrayList structFieldNames = new ArrayList();
		ArrayList structFieldInspectors = new ArrayList();
		structFieldNames.add("people");//struct的字段1
		structFieldNames.add("day");//struct的字段2
		structFieldNames.add("from_time");//struct的字段3
		structFieldNames.add("from_place");//struct的字段4
		structFieldNames.add("to_time");//struct的字段5
		structFieldNames.add("to_place");//struct的字段6
		structFieldInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		
		StructObjectInspector si2;
		si2 = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldInspectors);
		return si2;
	}

	//遍历每条记录  
    @Override  
    public Object evaluate(DeferredObject[] arguments) throws HiveException{  
     //A
     LazyString LPeople = (LazyString)(arguments[0].get());  
     String strPeople = ((StringObjectInspector)peopleObj).getPrimitiveJavaObject( LPeople );  
     //2015-3-24 8:000:00
     LazyString LTime = (LazyString)(arguments[1].get()); 
     String strTime = ((StringObjectInspector)timeObj).getPrimitiveJavaObject( LTime );  
     //home
     LazyString LPlace = (LazyString)(arguments[2].get());  
     String strPlace = ((StringObjectInspector)placeObj).getPrimitiveJavaObject( LPlace );  
     
     Object[] e;     
     e = new Object[6];  
 
         try  
     {  
               //如果是同一个人，同一天  
       if(strPrePeople.equals(strPeople) && IsSameDay(strTime) )  
       {  
               e[0] = new Text(strPeople);  						//people
                       e[1] = new Text(GetYearMonthDay(strTime));   //day
               e[2] = new Text(GetTime(strPreTime));  				//from_time
               e[3] = new Text(strPrePlace);  						//from_place
               e[4] = new Text(GetTime(strTime));  					//to_time
               e[5] = new Text(strPlace);  							//to_place
       }  
               else  
               {  
               e[0] = new Text(strPeople);  
           e[1] = new Text(GetYearMonthDay(strTime));  
               e[2] = new Text("null");  
               e[3] = new Text("null");  
               e[4] = new Text(GetTime(strTime));  
               e[5] = new Text(strPlace);  
               }  
         }  
         catch(java.text.ParseException ex)  
         {  
         }  
            
     strPrePeople = new String(strPeople);  
     strPreTime= new String(strTime);  
     strPrePlace = new String(strPlace);  
 
         return e;  
    }  
  
    @Override  
    public String getDisplayString(String[] children) {  
         assert( children.length>0 );  
  
         StringBuilder sb = new StringBuilder();  
         sb.append("helloGenericUDF(");  
         sb.append(children[0]);  
         sb.append(")");  
  
         return sb.toString();  
    }  
 
    //比较相邻两个时间段是否在同一天  
    private boolean IsSameDay(String strTime) throws java.text.ParseException{     
    if(strPreTime.isEmpty()){  
        return false;  
        }  
        String curDay = GetYearMonthDay(strTime);  
        String preDay = GetYearMonthDay(strPreTime);  
    return curDay.equals(preDay);  
    }  
 
    //获取年月日  
    private String GetYearMonthDay(String strTime)  throws java.text.ParseException{  
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
        Date curDate = df.parse(strTime);  
    df = new SimpleDateFormat("yyyy-MM-dd");  
        return df.format(curDate);  
    }  
 
    //获取时间  
    private String GetTime(String strTime)  throws java.text.ParseException{  
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
        Date curDate = df.parse(strTime);  
        df = new SimpleDateFormat("HH:mm:ss");  
        return df.format(curDate);  
    }  
}  