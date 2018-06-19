package demo.hadoop.job_control;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

public class JobControlTest {
	public static void main(String[] args) throws IOException {
		//配置jobox
		Configuration confx = new Configuration();
		Job jobx = new Job(confx, "jobx");
		//配置joboy
		Configuration confy = new Configuration();
		Job joby = new Job(confy, "joby");
		//配置joboz
		Configuration confz = new Configuration();
		Job jobz = new Job(confz, "jobz");
		//..job的其他设置
		/*设置job之间的依赖关系，需要嵌套job控制容器*/
		ControlledJob ctrlJobx = new ControlledJob(confx);
		ctrlJobx.setJob(jobx);
		ControlledJob ctrlJoby = new ControlledJob(confy);
		ctrlJobx.setJob(joby);
		ControlledJob ctrlJobz = new ControlledJob(confz);
		ctrlJobx.setJob(jobz);
		//设置jobz与jobx的依赖关系，jobz需要等待jobx执行完毕之后再执行
		ctrlJobz.addDependingJob(ctrlJobx);
		//设置jobz与joby的依赖关系，jobz需要等待joby执行完毕之后再执行
		ctrlJobz.addDependingJob(ctrlJoby);
		
		
		
	}
}
