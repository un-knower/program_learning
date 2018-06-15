package demo.quartz.demo6;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class BadJob1 implements Job {
    private int calculation;
    @Override
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
        JobKey jobKey = context.getJobDetail().getKey();
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();

        int denominator = dataMap.getInt("denominator");
        System.out.println("---" + dateFormat.format(new Date()) +":"+ jobKey + " 除数为： " + denominator);
        try {
            this.calculation = (4815 / denominator);
        } catch (Exception e) {
            System.out.println("--- Error in job!");
            JobExecutionException e2 = new JobExecutionException(e);

            dataMap.put("denominator", "1");

            //job会继续执行下去
            e2.setRefireImmediately(true);
            throw e2;
        }

        System.out.println("---" +dateFormat.format(new Date()) +":"+ jobKey  +" 执行结束 "  );
    }
}