package demo.quartz.demo1;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;


public class SchedulerDemo {

    public Scheduler getScheduler() throws SchedulerException {
        // 创建scheduler  JobDetail是任务的定义，而Job是任务的执行逻辑。在JobDetail里会引用一个Job Class定义。
        StdSchedulerFactory stdSchedulerFactory = new StdSchedulerFactory();
        return stdSchedulerFactory.getScheduler();
    }
    public void schedulerJob() throws SchedulerException, InterruptedException {
        // 创建任务
        JobDetail jobDetail = JobBuilder.newJob(MyJob.class)
                                        .withIdentity("job1", "group1") // 设置name/group
                                        .withDescription("this is description")
                                        .usingJobData("age", 18)
                                        .build();

        jobDetail.getJobDataMap().put("name", "quartz"); // 加入属性name到JobDataMap中
        // 创建触发器 每3秒执行一次
        SimpleTrigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger1", "group3") // 定义name/group
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(3).repeatForever())
                .build();
        Scheduler scheduler = getScheduler();

        // 将任务及其触发器放入调度器
        scheduler.scheduleJob(jobDetail, trigger);
        // 调度器开始调度任务
        scheduler.start();


        //运行一段时间后关闭
        Thread.sleep(10000);
        scheduler.shutdown(true);
    }
    public static void main(String args[]) throws SchedulerException, InterruptedException {
        SchedulerDemo mainScheduler = new SchedulerDemo();
        mainScheduler.schedulerJob();


    }
}

