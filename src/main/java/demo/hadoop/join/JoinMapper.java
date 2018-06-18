package demo.hadoop.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Employee>{
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] content = value.toString().split("\t");
		
		Employee employee = new Employee();
		if(content.length<=3) { //读取的是 dept文件,flag标记为1
			String deptno = content[0];
			String dname = content[1];
			employee.setDeptNo(deptno);
			employee.setDeptName(dname);
			employee.setFlag(1);
			context.write(new Text(deptno), employee);
		}
		else { //读取的是 employee文件,flag标记为0
			String empno = content[0];
			String ename = content[1];
			String deptno = content[7];
			
			employee.setEmpNo(empno);
			employee.setEmpName(ename);
			employee.setDeptNo(deptno);
			employee.setFlag(0);
			//这里由于文件格式有点问题（有的行末的deptno有空格，所以需要trim下，否则有空格和没空格的key不会匹配到一起）
			context.write(new Text(deptno.trim()), employee);  
		}
		
	}
}
