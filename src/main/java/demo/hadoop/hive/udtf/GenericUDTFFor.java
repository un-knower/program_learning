package demo.hadoop.hive.udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * 实现类似功能
 * hive>SELECT forx(1,5) AS i FROM src;
 * 1
 * 2
 * 3
 * 4
 * 5
 * @author qingjian
 *
 */
public class GenericUDTFFor extends GenericUDTF{
	
	IntWritable start;	//起始
	IntWritable end;	//结束
	IntWritable inc;	//增量
	Object[] forwardObj = null; //用于存放要返回的结果行
	
	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		start = ((WritableConstantIntObjectInspector)argOIs[0]).getWritableConstantValue();
		end = ((WritableConstantIntObjectInspector)argOIs[1]).getWritableConstantValue();
		if(argOIs.length==3) {
			inc = ((WritableConstantIntObjectInspector)argOIs[2]).getWritableConstantValue();
		}else {
			inc = new IntWritable(1);//默认为增量为1
		}
		
		this.forwardObj = new Object[1];
		//本函数只会返回一行数据，而且这行数据的数据类型确定是整型的。我们需要提供一个列明，不过用户通常可以在后面重新命名列名
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("col0");
//		fieldOIs.add(
//				PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.INT));
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
		//fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);//这么写有错
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	//实际进行处理的过程
	//这个方法的返回类型是void。这是因为UDTF可以向前获取零行或多行数据，而不像UDF，其只有唯一返回值。
	//这种情况下会在for循环中对forward方法进行多次调用，这样每迭代一次就可一获取一行数据。
	@Override
	public void process(Object[] args) throws HiveException {
		for(int i=start.get();i<end.get();i=i+inc.get()) {
			this.forwardObj[0]=new Integer(i);
			forward(forwardObj);
		}
	}

	@Override
	public void close() throws HiveException {
		
	}

}
