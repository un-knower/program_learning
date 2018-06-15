package demo.quartz.demo7;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;

public class SimpleJob2 implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
        JobKey jobKey = context.getJobDetail().getKey();
        System.out.println("SimpleJob2 : "+ dateFormat.format(new Date()) + jobKey + " 被执行 " );
    }
}
