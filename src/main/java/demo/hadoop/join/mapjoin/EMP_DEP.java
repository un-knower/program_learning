package demo.hadoop.join.mapjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class EMP_DEP implements Writable{
	private String name = "";
	private String sex = "";
	private int age = 0;
	private int DepNo = 0;
	private String DepName = "";
	private String table = ""; //用于区分数据的来源
	
	public EMP_DEP() {
	}
	public EMP_DEP(EMP_DEP o) {
		this.name = o.getName();
		this.sex = o.getSex();
		this.age = o.getAge();
		this.DepNo = o.getDepNo();
		this.DepName = o.getDepName();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.sex = in.readUTF();
		this.age = in.readInt();
		this.DepNo = in.readInt();
		this.DepName = in.readUTF();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(sex);
		out.writeInt(age);
		out.writeInt(DepNo);
		out.writeUTF(DepName);
	}

	
	@Override
	public String toString() {
		return name+"\t"+sex+"\t"+age+"\t"+DepNo+"\t"+DepName;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getSex() {
		return sex;
	}
	
	public void setSex(String sex) {
		this.sex = sex;
	}
	
	public int getAge() {
		return age;
	}
	
	public void setAge(int age) {
		this.age = age;
	}
	
	public int getDepNo() {
		return DepNo;
	}
	
	public void setDepNo(int depNo) {
		DepNo = depNo;
	}
	
	public String getDepName() {
		return DepName;
	}
	
	public void setDepName(String depName) {
		DepName = depName;
	}

}
