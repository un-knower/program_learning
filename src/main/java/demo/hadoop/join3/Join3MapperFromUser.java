package demo.hadoop.join3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Join3MapperFromUser extends Mapper<LongWritable, Text, Text, UserPhone>{

	//读取人员信息user文件
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, UserPhone>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		String name = split[0];
		String phone = split[1];
		int flag =0; //读取自user文件，flag为 0
		UserPhone userPhone = new UserPhone();
		userPhone.setName(name);
		userPhone.setPhone(phone);
		userPhone.setFlag(flag);
		context.write(new Text(phone), userPhone);
		
	}
	
}
