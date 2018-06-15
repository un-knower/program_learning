package demo.quartz.demo4;

import org.quartz.*;

import java.text.SimpleDateFormat;
import java.util.Date;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class BadJob1 implements Job {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobKey jobKey = context.getJobDetail().getKey();
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();

        int denominator = dataMap.getInt("denominator");
        System.out.println("---" + jobKey + " executing at " + simpleDateFormat.format(new Date()) + " with denominator " + denominator);

        denominator++;
        dataMap.put("denominator", denominator);


    }
}
