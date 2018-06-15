package demo.quartz.demo4;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Date;

/**
 * 定义两个job。这两个job是不能共享jobmap的，且identity不能相同
 */
public class JobExceptionExample {
    public void run() throws Exception {
        StdSchedulerFactory stdSchedulerFactory = new StdSchedulerFactory();
        Scheduler scheduler = stdSchedulerFactory.getScheduler();

// get a "nice round" time a few seconds in the future...
        Date startTime = DateBuilder.nextGivenSecondDate(null, 2);

        JobDetail job = JobBuilder.newJob(BadJob1.class).withIdentity("badJob1", "group1").usingJobData("denominator", "0").build();

        SimpleTrigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger1", "group1")
                                .startAt(startTime).withSchedule(SimpleScheduleBuilder.simpleSchedule()
                                .withIntervalInSeconds(2).repeatForever()).build();

        Date ft = scheduler.scheduleJob(job, trigger);

        // 任务每2秒执行一次 那么在BadJob1的方法中拿到的JobDataMap的数据是共享的.
        // 这里要注意一个情况： 就是JobDataMap的数据共享只针对一个BadJob1任务。
        // 如果在下面在新增加一个任务 那么他们之间是不共享的 比如下面

        JobDetail job2 = JobBuilder.newJob(BadJob1.class).withIdentity("badJob2", "group1")
                .usingJobData("denominator", "0").build();

        SimpleTrigger trigger2 = TriggerBuilder.newTrigger().withIdentity("trigger2", "group1")
                .startAt(startTime).withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(2).repeatForever()).build();

        //这个job2与job执行的JobDataMap不共享
        scheduler.scheduleJob(job2, trigger2);

        scheduler.start();

        try {
            // sleep for 30 seconds
            Thread.sleep(30L * 1000L);
        } catch (Exception e) {
        }

        scheduler.shutdown(false);

    }

    public static void main(String[] args) throws Exception {

        JobExceptionExample example = new JobExceptionExample();
        example.run();
    }
}


//输出结果
//        ---group1.badJob1 executing at 2018-06-06 16:02:38 with denominator 0
//        ---group1.badJob2 executing at 2018-06-06 16:02:38 with denominator 0
//        ---group1.badJob2 executing at 2018-06-06 16:02:40 with denominator 1
//        ---group1.badJob1 executing at 2018-06-06 16:02:40 with denominator 1
//        ---group1.badJob1 executing at 2018-06-06 16:02:42 with denominator 2
//        ---group1.badJob2 executing at 2018-06-06 16:02:42 with denominator 2
//        ---group1.badJob1 executing at 2018-06-06 16:02:44 with denominator 3
//        ---group1.badJob2 executing at 2018-06-06 16:02:44 with denominator 3
//        ---group1.badJob1 executing at 2018-06-06 16:02:46 with denominator 4
//        ---group1.badJob2 executing at 2018-06-06 16:02:46 with denominator 4
//        ---group1.badJob1 executing at 2018-06-06 16:02:48 with denominator 5
//        ---group1.badJob2 executing at 2018-06-06 16:02:48 with denominator 5
