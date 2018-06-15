package demo.quartz.demo8;

import java.text.SimpleDateFormat;
import java.util.Date;

import demo.quartz.demo7.Job1Listener;
import demo.quartz.demo7.SimpleJob1;
import org.quartz.DateBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Matcher;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.SchedulerMetaData;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.TriggerListener;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.KeyMatcher;


public class TriggerListenerExample {

    public void run() throws Exception {

        System.out.println("------- 初始化 -------------------");

        SchedulerFactory sf = new StdSchedulerFactory();
        Scheduler sched = sf.getScheduler();

        System.out.println("------- 初始化完成 --------");

        System.out.println("------- 向Scheduler加入Job ----------------");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");

        Date startTime = DateBuilder.nextGivenSecondDate(null, 15);

        JobDetail job = JobBuilder.newJob(SimpleJob1.class)
                .withIdentity("job1", "group1")
                .build();

        SimpleTrigger trigger = (SimpleTrigger)TriggerBuilder.newTrigger()
                .withIdentity("trigger1", "group1")
                .startAt(startTime)
                .withSchedule(SimpleScheduleBuilder
                        .simpleSchedule()
                        .withIntervalInSeconds(10)
                        .withRepeatCount(5))
                .build();

        JobListener jobListener = new Job1Listener();

        // 根据name和group 匹配一个job实例
        Matcher<JobKey> jobMatcher = KeyMatcher.keyEquals(job.getKey());

        // 为job添加监听器
        sched.getListenerManager().addJobListener(jobListener, jobMatcher);

        TriggerListener triggerListener = new MyTriggerListener();

        // 根据name和group 匹配一个trigger实例
        Matcher<TriggerKey> triggerMatcher = KeyMatcher.keyEquals(trigger.getKey());

        //为Trigger添加监听器
        sched.getListenerManager().addTriggerListener(triggerListener);

        Date ft = sched.scheduleJob(job, trigger);

        System.out.println(job.getKey() + " 将会在: " + dateFormat.format(ft) + "时运行，"
                + "重复: " + trigger.getRepeatCount() + " 次, " //获取重复的次数
                + "每 " + trigger.getRepeatInterval() / 1000L + " s 重复一次");
        System.out.println("------- 开始Scheduler ----------------");

        sched.start();

        System.out.println("------- Scheduler调用job结束 -----------------");
        try {
            Thread.sleep(30000L);
        } catch (Exception e) {
        }

        System.out.println("------- 关闭Scheduler ---------------------");
        sched.shutdown(true);
        System.out.println("------- 关闭完成 -----------------");

        SchedulerMetaData metaData = sched.getMetaData();
        System.out.println("Executed " + metaData.getNumberOfJobsExecuted() + " jobs.");
    }

    public static void main(String[] args) throws Exception {
        TriggerListenerExample example = new TriggerListenerExample();
        example.run();
    }
}
