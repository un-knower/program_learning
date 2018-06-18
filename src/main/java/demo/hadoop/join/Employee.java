package demo.hadoop.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Employee implements WritableComparable<Employee>{
	private String empNo="";  //这里必须初始化，否则在写的地方报错空指针
	private String empName="";
	private String deptNo="";
	private String deptName="";
	private int flag = 0; //employee员工数据文件数据为0，dept部门数据为1
	
	public Employee() {
	}
	public Employee(String empNo, String empName, String deptNo, String deptName,int flag) {
		this.empNo=empNo;
		this.empName=empName;
		this.deptName=empName;
		this.deptNo=deptNo;
		this.deptName=deptName;
		this.flag=flag;
	}
	
	/**
	 * 注意数组的加载对象
	 * @param e
	 */
	public Employee(Employee e) {
		this.empNo = e.getEmpNo();
		this.empName = e.getEmpName();
		this.deptNo = e.getDeptNo();
		this.deptName = e.getDeptName();
		this.flag = e.getFlag();
	}
	
	public String getEmpNo() {
		return empNo;
	}
	public void setEmpNo(String empNo) {
		this.empNo = empNo;
	}
	public String getEmpName() {
		return empName;
	}
	public void setEmpName(String empName) {
		this.empName = empName;
	}
	public String getDeptNo() {
		return deptNo;
	}
	public void setDeptNo(String deptNo) {
		this.deptNo = deptNo;
	}
	public String getDeptName() {
		return deptName;
	}
	public void setDeptName(String deptName) {
		this.deptName = deptName;
	}
	public int getFlag() {
		return flag;
	}
	public void setFlag(int flag) {
		this.flag = flag;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.empNo=in.readUTF();
		this.empName=in.readUTF();
		this.deptNo=in.readUTF();
		this.deptName=in.readUTF();
		this.flag=in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.empNo);
		out.writeUTF(this.empName);
		out.writeUTF(this.deptNo);
		out.writeUTF(this.deptName);
		out.writeInt(this.flag);
		
	}

	@Override
	public int compareTo(Employee o) {
		return 0;
	}
	
	public String toString() {
		return empNo+"\t"+empName+"\t"+deptNo+"\t"+deptName;
	}
	
}
