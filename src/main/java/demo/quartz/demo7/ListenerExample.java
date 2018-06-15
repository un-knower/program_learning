package demo.quartz.demo7;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobListener;
import org.quartz.Matcher;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.SchedulerMetaData;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.KeyMatcher;

/*
------- 初始化 -------------------
------- 初始化完成 --------
------- 向Scheduler加入Job ----------------
------- 开始Scheduler ----------------
------- Scheduler调用job结束 -----------------
Job1Listener : job1 将被执行.
SimpleJob1 : 2018年06月06日 17时40分34秒DEFAULT.job1 被执行
Job1Listener : job1被执行
SimpleJob2 : 2018年06月06日 17时40分34秒DEFAULT.job2 被执行
------- 关闭Scheduler ---------------------
------- 关闭完成 -----------------
Executed 2 jobs.
*/


public class ListenerExample
{
    public void run() throws Exception {

        System.out.println("------- 初始化 -------------------");

        SchedulerFactory sf = new StdSchedulerFactory();
        Scheduler sched = sf.getScheduler();

        System.out.println("------- 初始化完成 --------");

        System.out.println("------- 向Scheduler加入Job ----------------");

        JobDetail job = JobBuilder.newJob(SimpleJob1.class).withIdentity("job1").build();

        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger1").startNow().build();

        JobListener listener = new Job1Listener();

        //根据name和group 匹配一个job实例
        Matcher matcher = KeyMatcher.keyEquals(job.getKey());

        //为job添加监听器
        sched.getListenerManager().addJobListener(listener, matcher);

        sched.scheduleJob(job, trigger);

        System.out.println("------- 开始Scheduler ----------------");

        sched.start();

        System.out.println("------- Scheduler调用job结束 -----------------");
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
        ListenerExample example = new ListenerExample();
        example.run();
    }
}
