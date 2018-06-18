package demo.hadoop.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
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
 *
 *hive>insert overwrite table tmpResult 
 *	  >select hellogenericudf(people,time,place) from whereme;
 *
 *
hive> select hellogenericudf(people,time,place) from whereme;

{"people":null,"day":null,"from_time":null,"from_place":null,"to_time":null,"to_place":null}
{"people":"A","day":"2015-03-24","from_time":"08:00:00","from_place":"home","to_time":"10:00:00","to_place":"super market"}
{"people":"A","day":"2015-03-24","from_time":"10:00:00","from_place":"super market","to_time":"12:00:00","to_place":"kfc "}
{"people":"A","day":"2015-03-24","from_time":"12:00:00","from_place":"kfc ","to_time":"15:00:00","to_place":"school"}
{"people":"A","day":"2015-03-24","from_time":"15:00:00","from_place":"school","to_time":"20:00:00","to_place":"home"}
{"people":null,"day":null,"from_time":null,"from_place":null,"to_time":null,"to_place":null}
{"people":"A","day":"2015-03-25","from_time":"08:00:00","from_place":"home","to_time":"10:00:00","to_place":"park"}
{"people":"A","day":"2015-03-25","from_time":"10:00:00","from_place":"park","to_time":"12:00:00","to_place":"home"}
{"people":"A","day":"2015-03-25","from_time":"12:00:00","from_place":"home","to_time":"15:30:00","to_place":"bank"}
{"people":"A","day":"2015-03-25","from_time":"15:30:00","from_place":"bank","to_time":"19:00:00","to_place":"home"}

 *
 *
 *
 *
 * hive> insert overwrite table whereresult                           
    > select info from tmpResult where info.from_time<>'null'; 
 *select * from whereresult;
 */
public class HelloGenericUDF2 extends GenericUDF{
	private StringObjectInspector peopleOI;
	private StringObjectInspector timeOI;
	private StringObjectInspector placeOI;
	//保存之前的数据
	private String prePeople;
	private String preTime;
	private String prePlace;
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		/*
		 * 定义输入变量解析器
		 */
		peopleOI = (StringObjectInspector) arguments[0];
		timeOI = (StringObjectInspector) arguments[1];
		placeOI = (StringObjectInspector) arguments[2];
		/*
		 * 定义输出，类型是struct，由两个list组成
		 */
		List<String> structFieldNames = new ArrayList<String>();
		List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
		structFieldNames.add("people");
		structFieldNames.add("day");
		structFieldNames.add("from_time");
		structFieldNames.add("from_place");
		structFieldNames.add("to_time");
		structFieldNames.add("to_place");
		//structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
	}

	//遍历每条数据,返回该条数据对应的一条结果
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		
		Object peopleObj = arguments[0].get();
		Object timeObj = arguments[1].get();
		Object placeObj = arguments[2].get();
		String peopleStr = peopleOI.getPrimitiveJavaObject(peopleObj);
		String timeStr = timeOI.getPrimitiveJavaObject(timeObj);
		String placeStr = placeOI.getPrimitiveJavaObject(placeObj);
		//结果
		Object[] resultObj = new Object[6];
		try {
			if(peopleStr!=null&&peopleStr.equals(prePeople)&&timeStr!=null&&getYearMonthDay(timeStr).equals(getYearMonthDay(preTime))) {
				resultObj[0]= new Text(prePeople); //people
				resultObj[1]= new Text(getYearMonthDay(timeStr)); //day
				resultObj[2]= new Text(getHourMinuSec(preTime)); //from_time 
				resultObj[3]= new Text(prePlace); //from_place
				resultObj[4]= new Text(getHourMinuSec(timeStr)); //to_time
				resultObj[5]= new Text(placeStr); //to_place
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}

		prePeople=peopleStr;
		preTime=timeStr;
		prePlace=placeStr;
		return resultObj;
	}

	@Override
	public String getDisplayString(String[] children) {
		return null;
	}
	public String getYearMonthDay(String time) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = sdf.parse(time);
		sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(date);
	}
	public String getHourMinuSec(String time) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = sdf.parse(time);
		sdf = new SimpleDateFormat("HH:mm:ss");
		return sdf.format(date);
	}

}
