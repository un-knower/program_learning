package demo.hadoop.join3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Join3MapperFromPhone extends Mapper<LongWritable, Text, Text, UserPhone>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, UserPhone>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		String phone = split[0];
		String desc = split[1];
		int flag = 1;//读取自phone文件，flag为1
		UserPhone userPhone = new UserPhone();
		userPhone.setPhone(phone);
		userPhone.setDesc(desc);
		userPhone.setFlag(flag);
		context.write(new Text(phone), userPhone);
	}
	
}
