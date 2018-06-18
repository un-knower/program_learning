package demo.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
/**
 * 
 * UDF分为简单UDF和通用UDF
 * 这是通用GenericUDF，需要(1)继承GenericUDF,(2)重写initialize,getDisplayString,evaluate方法
 * 
 * ObjectInspector辅助类帮助使用者访问需要序列化或者反序列化的对象
 * 
 * 计算array中去重后元素个数
 * @author qingjian
 *
 */
@Description(name="array_uniq_element_number",
			 value="_FUNC_(array)-Returns number of objects with duplicate elements eliminated.",
			 extended="Example:\n"
			 		+ ">SELECT _FUNC_(array(1,2,2,3,3)) FROM src LIMIT 1;\n"
			 		+ "	3"
)
public class UDFArrayUniqElementNumber extends GenericUDF{

	private static final int ARRAY_IDX = 0;
	private static final int ARG_COUNT = 1; //number of arguments to this UDF
	private static final String FUNC_NAME = "ARRAY_UNIQ_ELEMENT_NUMBER";//External Name
	private ListObjectInspector arrayOI;
	//用arrayElementOI来取arrayOI中的值
	private ObjectInspector arrayElementOI; //ListObjectInspector中包换一个或多个ObjectInspector
	private final IntWritable result = new IntWritable(-1);
	
	/*
	 * initialize()方法会被输入的每个参数调用，并最终传入到一个ObjectInspector对象中。
	 * 这个方法的目的是确定参数的返回类型。
	 * (1)确认输入类型是否正确
	 * (2)输出类型的定义
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		//1.判断参数个数
		//Check if two arguments were passed
		if(arguments.length!=ARG_COUNT) {
			throw new UDFArgumentException("The function "+FUNC_NAME
					+" accepts "+ARG_COUNT+ "arguments.");
		}
		//2.判断参数类型
		//Check if ARRAY_IDX argument is of category LIST
//		if(!arguments[ARRAY_IDX].getCategory().equals(Category.LIST)) {
//			//第几个参数类型不对，错误信息
//			//throw new UDFArgumentTypeException(argumentId, message)
//			throw new UDFArgumentTypeException(ARRAY_IDX, "\""
//					+ "org.apache.hadoop.hive.serde.Constants.LIST_TYPE_NAME\""
//					+ " expected at function ARRAY_CONTANS, but \""
//					+ " "+arguments[ARRAY_IDX].getTypeName()+" \""
//							+ "is found");
//		}
		//3.转换参数类型,初始化
		arrayOI = (ListObjectInspector) arguments[ARRAY_IDX];
		arrayElementOI = arrayOI.getListElementObjectInspector();

		return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
	}
 
	/*
	 *遍历每条记录,进行计算
	 *方法evaluate的输入是一个DeferredObject对象数组，
	 */
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		result.set(0);
		Object array = arguments[ARRAY_IDX].get();//得到该参数对象
		int arrayLength = arrayOI.getListLength(array);
		if(arrayLength<=1) {
			result.set(arrayLength);
			return result;
		}
		//element compare:Algorithm complexity:O(N^2)
		int num=1;
		int i,j;
		for(i=1;i<arrayLength;i++) {
			Object listElement = arrayOI.getListElement(array, i);
			for(j=i-1;j>=0;j--) {
				if(listElement!=null) {
					Object tmp = arrayOI.getListElement(array, j);
					/**
					   * Compare two objects with their respective ObjectInspectors.
					   */
					if(ObjectInspectorUtils.compare(tmp,arrayElementOI,listElement,arrayElementOI)==0) {
						break;
					}
				}
			}
			if(-1==j) {//未找到与listElement相同的，增加1
				num++;
			}
		}
		result.set(num);
		return result;
	}

	/*
	 *getDisplayString()方法，其用于Hadoop task内部，在使用到这个函数时来展示调试信息 
	 */
	@Override
	public String getDisplayString(String[] children) {
		assert(children.length==ARG_COUNT);
		return "array_uniq_element_number( "+children[ARRAY_IDX]+" )";
	}

}
