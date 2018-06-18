package demo.hadoop.join.mapjoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJoinMap extends Mapper<LongWritable, Text, NullWritable, EMP_DEP>{
	private Map<Integer, String> joinData = new HashMap<Integer, String>();
	EMP_DEP emp_dep = new EMP_DEP();
	BufferedReader reader = null;
	FSDataInputStream in = null;
	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, EMP_DEP>.Context context)
			throws IOException, InterruptedException {
		String str = null;
		URI[] localCacheFiles = context.getCacheFiles();
		for (URI uri : localCacheFiles) {
			FileSystem fileSystem = FileSystem.get(uri,context.getConfiguration());
			in = fileSystem.open(new Path(uri));
			reader = new BufferedReader(new InputStreamReader(in));
			while((str = reader.readLine())!=null) {
				String[] split = str.split("\\s+");
				joinData.put(Integer.parseInt(split[0]), split[1]);
			}
		}
		
	
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, EMP_DEP>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("\\s+");
		emp_dep.setName(split[0]);
		emp_dep.setSex(split[1]);
		emp_dep.setAge(Integer.parseInt(split[2]));
		int depNo = Integer.parseInt(split[3]);
		String depName = joinData.get(depNo);
		emp_dep.setDepNo(depNo);
		emp_dep.setDepName(depName);
		context.write(NullWritable.get(), emp_dep);
	}
	
	
	
}
