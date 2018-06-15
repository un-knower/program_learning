package demo.quartz.demo6;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.quartz.DateBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.SchedulerMetaData;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

/**
 *
 * JobExecutionException e2 = new JobExecutionException(e);
 *  e2.setRefireImmediately(true);    job会继续执行下去,下次继续调用
 *  e2.setRefireImmediately(false);    自动停止Schedule,和这个Job有关的触发器，Job也将不再运行
 *
 *
 */
public class JobExceptionExample
{
    public void run()
            throws Exception
    {

        System.out.println("------- 初始化 -------------------");

        SchedulerFactory sf = new StdSchedulerFactory();
        Scheduler sched = sf.getScheduler();

        System.out.println("------- 初始化完成 --------");

        System.out.println("------- 向Scheduler加入Job ----------------");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");

        Date startTime = DateBuilder.nextGivenSecondDate(null, 15);

        JobDetail job = JobBuilder.newJob(BadJob1.class)
                .withIdentity("badJob1", "group1")
                //设置一个为0的除数放入JobDatamap中
                .usingJobData("denominator", "0")
                .build();

        SimpleTrigger trigger = (SimpleTrigger)TriggerBuilder.newTrigger()
                .withIdentity("trigger1", "group1")
                .startAt(startTime)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10).repeatForever())
                .build();

        Date ft = sched.scheduleJob(job, trigger);
        System.out.println(job.getKey() + " 将在: " + dateFormat.format(ft) + " 时运行，重复: " + trigger.getRepeatCount() + " 次,每 " + trigger.getRepeatInterval() / 1000L + " 秒执行一次");

        job = JobBuilder.newJob(BadJob2.class).withIdentity("badJob2", "group1").build();

        trigger = (SimpleTrigger)TriggerBuilder.newTrigger()
                .withIdentity("trigger2", "group1")
                .startAt(startTime)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(5).repeatForever())
                .build();

        ft = sched.scheduleJob(job, trigger);
        System.out.println(job.getKey() + " 将在: " + dateFormat.format(ft) + " 时运行，重复: " + trigger.getRepeatCount() + " 次,每 " + trigger.getRepeatInterval() / 1000L + " 秒执行一次");

        System.out.println("------- 开始Scheduler ----------------");

        sched.start();

        System.out.println("------- Scheduler调用job结束 -----------------");

        System.out.println("------- 等待30秒... --------------");
        try{
            Thread.sleep(30000L);
        }
        catch (Exception e){
        }
        System.out.println("------- 关闭Scheduler ---------------------");

        sched.shutdown(true);

        System.out.println("------- 关闭完成 -----------------");

        SchedulerMetaData metaData = sched.getMetaData();
        System.out.println("Executed " + metaData.getNumberOfJobsExecuted() + " jobs.");
    }

    public static void main(String[] args) throws Exception{
        JobExceptionExample example = new JobExceptionExample();
        example.run();
    }
}
