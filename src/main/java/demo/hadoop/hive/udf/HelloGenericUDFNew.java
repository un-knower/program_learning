package demo.hadoop.hive.udf;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;  
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;  
import org.apache.hadoop.hive.ql.metadata.HiveException;  
import org.apache.hadoop.hive.serde2.lazy.LazyString;  
import org.apache.hadoop.hive.serde2.lazy.LazyMap;  
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;  
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;  
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;  
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;  
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;  
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;  
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;  
import java.util.ArrayList;  
/**
 * hive> describe tb_test2;  
OK  
name    string    
score_list  array<map<string,int>>    
Time taken: 0.074 seconds  
hive> select * from tb_test2;  
OK  
A   [{"math":100,"english":90,"history":85}]  
B   [{"math":95,"english":80,"history":100}]  
C   [{"math":80,"english":90,"histroy":100}]  
Time taken: 0.107 seconds  

嵌套的表创建过程
hive> create table tb_test(name stirng, score_list array<map<string,int>>);

hive> create table tb_test2_simple(name string,score map<string,int>)
    > row format delimited                                           
    > fields terminated by '\t'                                      
    > collection items terminated by ','                             
    > map keys terminated by ':';  

tb_test2_simple文件
A	math:100,english:90,history:85
B	math:95,english:80,history:100
C	math:80,english:90,history:100

hive> load data local inpath '/home/qingjian/桌面/tb_test2_simple' into table tb_test2_simple;

hive> insert into table tb_test2
    > select name,array(score)  
    > from tb_test2_simple; 
    

hive> select * from tb_test2;       
OK
A	[{"history":85,"english":90,"math":100}]
B	[{"history":100,"english":80,"math":95}]
C	[{"history":100,"english":90,"math":80}]


执行结果：
hive>select hellonew(tb_test2.name,tb_test2.score_list) from tb_test2;

{"name":"A","totalscore":275}
{"name":"B","totalscore":275}
{"name":"C","totalscore":270}

 * @author qingjian
 *
 */

   
public class HelloGenericUDFNew extends GenericUDF {  
     ////输入变量定义  
     private ObjectInspector nameObj;  
     private ListObjectInspector listoi;  
     private MapObjectInspector mapOI;  
     private ArrayList<Object> valueList = new ArrayList<Object>();   //成绩列表
     @Override  
     public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {  
          nameObj = (ObjectInspector)arguments[0];  
          listoi = (ListObjectInspector)arguments[1];
          
      mapOI = ((MapObjectInspector)listoi.getListElementObjectInspector());  
          //输出结构体定义  
          ArrayList structFieldNames = new ArrayList();  
          ArrayList structFieldObjectInspectors = new ArrayList();  
          structFieldNames.add("name");  
      structFieldNames.add("totalScore");  
   
          structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableStringObjectInspector );  
          structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableIntObjectInspector );  
  
          StructObjectInspector si2;  
          si2 = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);   
          return si2;  
     }  
   
     @Override  
     public Object evaluate(DeferredObject[] arguments) throws HiveException{  
      LazyString LName = (LazyString)(arguments[0].get());  
      String strName = ((StringObjectInspector)nameObj).getPrimitiveJavaObject( LName );  
  
      int nelements = listoi.getListLength(arguments[1].get());  //第一层列表长度
          int nTotalScore=0;  
          valueList.clear();  
          //遍历list  
      for(int i=0;i<nelements;i++)  
      {   
               LazyMap LMap = (LazyMap)listoi.getListElement(arguments[1].get(),i);  
               //获取map中的所有value值  
           valueList.addAll(mapOI.getMap(LMap).values());   
               for (int j = 0; j < valueList.size(); j++)  
           {  
                   nTotalScore+=Integer.parseInt(valueList.get(j).toString());  
               }                 
          }  
      Object[] e;     
      e = new Object[2];  
      e[0] = new Text(strName);  
          e[1] = new IntWritable(nTotalScore);  
          return e;  
     }  
   
     @Override  
     public String getDisplayString(String[] children) {  
          assert( children.length>0 );  
   
          StringBuilder sb = new StringBuilder();  
          sb.append("helloGenericUDFNew(");  
          sb.append(children[0]);  
          sb.append(")");  
   
          return sb.toString();  
     }  
}  