package demo.quartz.demo6;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class BadJob2 implements Job {
    private int calculation;
    @Override
    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
        JobKey jobKey = context.getJobDetail().getKey();
        System.out.println("---" +dateFormat.format(new Date()) +":"+ jobKey  );
        try {
            int zero = 0;
            this.calculation = (4815 / zero);
        } catch (Exception e) {
            System.out.println("--- Error in job!");
            JobExecutionException e2 = new JobExecutionException(e);

            //自动停止Schedule,和这个Job有关的触发器，Job也将不再运行
            e2.setUnscheduleAllTriggers(true);
            throw e2;
        }

        System.out.println("---" +dateFormat.format(new Date()) +":"+ jobKey  +" 执行结束 "  );
    }
}
