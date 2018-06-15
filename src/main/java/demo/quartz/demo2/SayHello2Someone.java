package demo.quartz.demo2;

import org.quartz.*;

/**
 * Job都次都是newInstance的实例，那我怎么传值给它？ 比如我现在有两个发送邮件的任务，一个是发给"liLei",一个发给"hanmeimei",不能说我要写两个Job实现类LiLeiSendEmailJob和HanMeiMeiSendEmailJob。实现的办法是通过JobDataMap。

 每一个JobDetail都会有一个JobDataMap。JobDataMap本质就是一个Map的扩展类，只是提供了一些更便捷的方法，比如getString()之类的。

 我们可以在定义JobDetail，加入属性值，方式有二：

 newJob().usingJobData("age", 18)
 或者
 job.setJobDataMap().put("name", "quartz")


 对于同一个JobDetail实例，执行的多个Job实例，是共享同样的JobDataMap，也就是说，如果你在任务里修改了里面的值，会对其他Job实例（并发的或者后续的）造成影响。

 除了JobDetail，Trigger同样有一个JobDataMap，共享范围是所有使用这个Trigger的Job实例。

 */
public class SayHello2Someone implements Job {
    private String name;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDetail jobDetail = jobExecutionContext.getJobDetail();
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        System.out.println("say hello to " + name + "[" + jobDataMap.getInt("age") + "]");
    }

    public void setName(String name) {
        this.name = name;
    }
}
