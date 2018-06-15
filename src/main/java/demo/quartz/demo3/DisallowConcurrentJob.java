package demo.quartz.demo3;

import org.quartz.*;

/**
 * Quartz定时任务默认都是并发执行的，不会等待上一次任务执行完毕，只要间隔时间到就会执行, 如果定时任执行太长，会长时间占用资源，导致其它任务堵塞。
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class DisallowConcurrentJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        System.out.println("do nothing");
    }
}
