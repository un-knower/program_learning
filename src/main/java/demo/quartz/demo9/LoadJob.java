package demo.quartz.demo9;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadJob
        implements Job
{
    public static final String DELAY_TIME = "delay time";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
        JobKey jobKey = context.getJobDetail().getKey();
        System.out.println(dateFormat.format(new Date()) + " : " + jobKey + " 执行 ");

        long delayTime = context.getJobDetail().getJobDataMap().getLong("delay time");

        try {
            Thread.sleep(delayTime);
        }
        catch (Exception e)
        {
        }
        System.out.println(dateFormat.format(new Date()) + " : " + jobKey + "完成");
    }
}
