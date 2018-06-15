package demo.quartz.demo3;

import org.quartz.*;

import java.util.Date;

/**
 * 设定的时间间隔为3秒,但job执行时间是10秒,
 * 设置@DisallowConcurrentExecution以后程序会等任务执行完毕以后再去执行,否则会在3秒时再启用新的线程执行
 *
 *

 org.quartz.threadPool.threadCount = 5 这里配置框架的线程池中线程的数量,要多配置几个,否则@DisallowConcurrentExecution不起作用
 org.quartz.scheduler.instanceName = MyScheduler
 org.quartz.threadPool.threadCount = 5
 org.quartz.jobStore.class =org.quartz.simpl.RAMJobStore

 @PersistJobDataAfterExecution

 此标记说明在执行完Job的execution方法后保存JobDataMap当中固定数据,在默认情况下 也就是没有设置 @PersistJobDataAfterExecution的时候 每个job都拥有独立JobDataMap

 否则该任务在重复执行的时候具有相同的JobDataMap



1） 保留@PersistJobDataAfterExecution和@DisallowConcurrentExecution，输出结果为。每个job共享JobDataMap，任务之间 不并发
 ---group1.statefulJob1 executing. [Wed Jun 06 15:47:30 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:47:42 CST 2018]
 ---group1.statefulJob1 complete. [2]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:47:54 CST 2018]
 ---group1.statefulJob1 complete. [3]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:48:06 CST 2018]
 ---group1.statefulJob1 complete. [4]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:48:18 CST 2018]
 ---group1.statefulJob1 complete. [5]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:48:30 CST 2018]
 ---group1.statefulJob1 complete. [6]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:48:42 CST 2018]
 ---group1.statefulJob1 complete. [7]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:48:54 CST 2018]
 ---group1.statefulJob1 complete. [8]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:49:06 CST 2018]
 ---group1.statefulJob1 complete. [9]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:49:18 CST 2018]
 ---group1.statefulJob1 complete. [10]



 2） 如果注释掉@PersistJobDataAfterExecution，保留@DisallowConcurrentExecution，输出结果为。每个job都拥有独立JobDataMap，但是任务之间 不并发
 ---group1.statefulJob1 executing. [Wed Jun 06 15:12:45 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:12:57 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:13:09 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:13:21 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:13:33 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:13:45 CST 2018]

 3）如果注释掉@DisallowConcurrentExecution，保留@PersistJobDataAfterExecution，输出结果为。3秒一个任务，但是每个job共享JobDataMap
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:00 CST 2018]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:03 CST 2018]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:06 CST 2018]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:09 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:12 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:15 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:18 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:21 CST 2018]
 ---group1.statefulJob1 complete. [2]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:24 CST 2018]
 ---group1.statefulJob1 complete. [2]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:27 CST 2018]
 ---group1.statefulJob1 complete. [2]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:30 CST 2018]
 ---group1.statefulJob1 complete. [2]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:33 CST 2018]
 ---group1.statefulJob1 complete. [3]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:36 CST 2018]
 ---group1.statefulJob1 complete. [3]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:39 CST 2018]
 ---group1.statefulJob1 complete. [3]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:42 CST 2018]
 ---group1.statefulJob1 complete. [3]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:45 CST 2018]
 ---group1.statefulJob1 complete. [4]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:48 CST 2018]
 ---group1.statefulJob1 complete. [4]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:51 CST 2018]
 ---group1.statefulJob1 complete. [4]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:54 CST 2018]
 ---group1.statefulJob1 complete. [4]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:16:57 CST 2018]
 ---group1.statefulJob1 complete. [5]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:00 CST 2018]
 ---group1.statefulJob1 complete. [5]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:03 CST 2018]
 ---group1.statefulJob1 complete. [5]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:06 CST 2018]
 ---group1.statefulJob1 complete. [5]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:09 CST 2018]
 ---group1.statefulJob1 complete. [6]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:12 CST 2018]
 ---group1.statefulJob1 complete. [6]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:15 CST 2018]
 ---group1.statefulJob1 complete. [6]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:18 CST 2018]
 ---group1.statefulJob1 complete. [6]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:21 CST 2018]
 ---group1.statefulJob1 complete. [7]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:24 CST 2018]
 ---group1.statefulJob1 complete. [7]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:27 CST 2018]
 ---group1.statefulJob1 complete. [7]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:17:30 CST 2018]
 ---group1.statefulJob1 complete. [7]

 4） 如果同时注释@PersistJobDataAfterExecution和@DisallowConcurrentExecution，输出结果为。每个job都拥有独立JobDataMap
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:15 CST 2018]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:18 CST 2018]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:21 CST 2018]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:24 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:27 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:30 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:33 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:36 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:39 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:42 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:45 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:48 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:51 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:54 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:14:57 CST 2018]
 ---group1.statefulJob1 complete. [1]
 ---group1.statefulJob1 executing. [Wed Jun 06 15:15:00 CST 2018]


 */
//@PersistJobDataAfterExecution
//@DisallowConcurrentExecution
public class StatefulDumbJob implements Job {

    public static final String NUM_EXECUTIONS = "NumExecutions";
    public static final String EXECUTION_DELAY = "ExecutionDelay";
    public StatefulDumbJob() {
    }


    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        System.err.println("---" + context.getJobDetail().getKey() + " executing. [" + new Date()+"]");    // getKey() =>  group.name
        JobDataMap map = context.getJobDetail().getJobDataMap();

        int executeCount = 0;
        if (map.containsKey(NUM_EXECUTIONS)) {
            executeCount = map.getInt(NUM_EXECUTIONS);
        }
        executeCount ++ ;

        map.put(NUM_EXECUTIONS, executeCount);

        long delay = 5000L; // 延迟5s
        if (map.containsKey(EXECUTION_DELAY)) {
            delay = map.getLong(EXECUTION_DELAY);
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.err.println("---" + context.getJobDetail().getKey() + " complete. [" + executeCount +"]");

    }
}
