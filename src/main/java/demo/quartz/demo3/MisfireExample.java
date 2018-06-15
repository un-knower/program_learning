package demo.quartz.demo3;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;


public class MisfireExample {
    public void run() throws Exception {
        Logger log = LoggerFactory.getLogger(MisfireExample.class);
        System.out.println("------- Initializing -------------------");

        // First we must get a reference to a scheduler
        StdSchedulerFactory sf = new StdSchedulerFactory();
        Properties props = new Properties();
        props.put("org.quartz.threadPool.threadCount", "10");
        props.put("org.quartz.scheduler.instanceName", "MyScheduler");
        props.put("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");

        sf.initialize(props);
        Scheduler scheduler = sf.getScheduler();


        System.out.println("------- Initialization Complete -----------");
        System.out.println("------- Scheduling Jobs -----------");

        // jobs can be scheduled before start() has been called
        Date startTime = DateBuilder.nextGivenSecondDate(null, 15);   // 下一个15秒

        // get a "nice round" time a few seconds in the future...

        // statefulJob1 will run every three seconds
        // (but it will delay for ten seconds)
        JobDetail job = JobBuilder.newJob(StatefulDumbJob.class)
                    .withIdentity("statefulJob1", "group1")
                    .usingJobData(StatefulDumbJob.EXECUTION_DELAY, 10000L).build();

        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("trigger1", "group1")
                .startAt(startTime)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(3).repeatForever())
                .build();
        Date firstFireTime = scheduler.scheduleJob(job, trigger);
        System.out.println(job.getKey() + " will run at: " + firstFireTime + " and repeat: " + trigger.getRepeatCount() + " times, every " + trigger.getRepeatInterval() / 1000 + " seconds");
        System.out.println("------- Starting Scheduler ----------------");

        // jobs don't start firing until start() has been called...
        scheduler.start();

        System.out.println("------- Started Scheduler -----------------");
        try {
            // sleep for ten minutes for triggers to file....
            Thread.sleep(600L * 1000L);  // 600s
        } catch (Exception e) {
        }

        System.out.println("------- Shutting Down ---------------------");

        scheduler.shutdown(true);

        System.out.println("------- Shutdown Complete -----------------");

        SchedulerMetaData metaData = scheduler.getMetaData();
        System.out.println("Executed " + metaData.getNumberOfJobsExecuted() + " jobs.");

    }
    public static void main(String[] args) throws Exception {

        MisfireExample example = new MisfireExample();
        example.run();
    }
}
