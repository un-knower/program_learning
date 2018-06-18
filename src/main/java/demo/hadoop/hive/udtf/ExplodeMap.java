package demo.hadoop.hive.udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 用来切分"key:value;key:value"这种字符串。返回结果为key,value两个字段
 * 
 * hive> create table explodemap(id string);
 * hive> load data local inpath '/home/qingjian/桌面/eplodemap' overwrite into explodemap;
 * 
 * hive> select * from explodemap;

key1:value1;key2:value2;
key3:value3;key3:value3;

 * hive> select explode_map(id) as (i,j) from explodemap;  
 * 
key1	value1
key2	value2
key3	value3
key4	value4

 * 
 * 
 * @author qingjian
 *
 */
public class ExplodeMap extends GenericUDTF{

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		//验证参数
		//检验参数个数
		if(argOIs.length!=1) {
			throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
		}
		//检验参数类型
		if(argOIs[0].getCategory()!=ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException("ExplodeMap takes string as a parameter");
		}
		
		//创建输出的两个字段
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("col1");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("col2");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		String input = args[0].toString();
		String[] test = input.split(";");
		for(int i=0;i<test.length;i++) {
			String[] result = test[i].split(":");
			//Passes an output row to the collector
			forward(result);
		}
		
	}

	@Override
	public void close() throws HiveException {
		
	}

}
