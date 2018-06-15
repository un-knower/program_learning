package demo.quartz.demo7;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

/**
 * 一个Job监听器
 *
 * 在job1中调用job2
 */
public class Job1Listener implements JobListener {
    @Override
    public String getName() {
        return "job1_to_job2";
    }

    @Override
    //Scheduler 在 JobDetail 将要被执行时调用这个方法。
    public void jobToBeExecuted(JobExecutionContext inContext) {
        System.out.println("Job1Listener : "+inContext.getJobDetail().getKey().getName()+" 将被执行. ");
    }

    @Override
    //Scheduler 在 JobDetail 即将被执行，但又被 TriggerListener 否决了时调用这个方法。
    public void jobExecutionVetoed(JobExecutionContext inContext) {
        System.out.println("Job1Listener : "+inContext.getJobDetail().getKey().getName()+" 被否决  ");
    }

    @Override
    //Scheduler 在 JobDetail 被执行之后调用这个方法。
    public void jobWasExecuted(JobExecutionContext inContext, JobExecutionException inException) {
        System.out.println("Job1Listener : "+inContext.getJobDetail().getKey().getName()+"被执行");

        JobDetail job2 = JobBuilder.newJob(SimpleJob2.class).withIdentity("job2").build();

        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("job2Trigger").startNow().build();
        try{
            inContext.getScheduler().scheduleJob(job2, trigger);
        } catch (SchedulerException e) {
            System.err.println(" job2无法执行! ");
            e.printStackTrace();
        }
    }
}
