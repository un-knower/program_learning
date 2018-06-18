package demo.hadoop.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class JoinReducer extends Reducer<Text, Employee, Text, NullWritable>{
	
	
	protected void reduce(Text key, Iterable<Employee> values,Context context) throws IOException, InterruptedException {
		List<Employee> employeeList = new ArrayList<Employee>();
		Employee employee = null; //用于保存deptno 和 dname
		for(Employee e : values) { //遍历得到employee员工列表
			if(e.getFlag()==0) {
//				employeeList.add(e); 这里直接指向不对
				employeeList.add(new Employee(e));
			}
			else {
//				employee=e;  这里直接指向不对
				employee=new Employee(e);
			}
		}
		
		if(employee!=null) {
			for(Employee e:employeeList) {
				e.setDeptName(employee.getDeptName());
				context.write(new Text(e.toString()), NullWritable.get());
			}
		}
		
	}
}
