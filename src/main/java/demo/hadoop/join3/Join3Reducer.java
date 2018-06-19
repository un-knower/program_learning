package demo.hadoop.join3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Join3Reducer extends Reducer<Text, UserPhone, NullWritable, Text>{

	@Override
	protected void reduce(Text key, Iterable<UserPhone> values,
			Reducer<Text, UserPhone, NullWritable, Text>.Context context) throws IOException, InterruptedException {
		
		List<UserPhone> userPhoneList = new ArrayList<UserPhone>();
		UserPhone userPhone = null; //保存来自user文件的内容
		for(UserPhone u:values) {
			if(u.getFlag()==0) { //内容来自user文件，内容唯一
				userPhone = new UserPhone(u); 
			}
			else {
				userPhoneList.add(new UserPhone(u));
			}
		}
		if(userPhone != null) {
			for(UserPhone u : userPhoneList) {
				u.setName(userPhone.getName());
				context.write(NullWritable.get(), new Text(u.toString()));
			}
		}
	}
}
