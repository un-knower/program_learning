package demo.hadoop.join3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserPhone implements WritableComparable<UserPhone>{
	private String name = "";
	private String phone = "";
	private String desc = "";
	private int flag = 0; //0来自人员信息文件，1来自电话信息文件。
	//因为使用多个输入格式
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getFlag() {
		return flag;
	}
	public void setFlag(int flag) {
		this.flag = flag;
	}
	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}
	public UserPhone() {
	}
	
	public UserPhone(String name, String phone, String desc, int flag) {
		this.name = name;
		this.phone = phone;
		this.desc = desc;
		this.flag = flag;
	}
	public UserPhone(UserPhone userPhone) {
		this.name = userPhone.getName();
		this.phone = userPhone.getPhone();
		this.desc = userPhone.getDesc();
		this.flag = userPhone.getFlag();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.phone = in.readUTF();
		this.desc = in.readUTF();
		this.flag = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(phone);
		out.writeUTF(desc);
		out.writeInt(flag);
		
	}

	@Override
	public int compareTo(UserPhone o) {
		return 0;
	}
	
	@Override
	public String toString() {
		return name+"\t"+phone+"\t"+desc;
	}
	

}
