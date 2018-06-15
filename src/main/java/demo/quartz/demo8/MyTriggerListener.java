package demo.quartz.demo8;

import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.TriggerListener;

/**
 * trigger监听器
 *
 *
 ------- 初始化完成 --------
 ------- 向Scheduler加入Job ----------------
 group1.job1 将会在: 2018年06月06日 17时55分00秒时运行，重复: 5 次, 每 10 s 重复一次
 ------- 开始Scheduler ----------------
 ------- Scheduler调用job结束 -----------------
 MyTriggerListener :trigger1 被执行
 不执行Job
 Job1Listener : job1 被否决

 MyTriggerListener :trigger1 被执行
 执行Job
 Job1Listener : job1 将被执行.
 SimpleJob1 : 2018年06月06日 17时55分10秒group1.job1 被执行
 Job1Listener : job1被执行
 Job执行完毕,Trigger完成
 MyTriggerListener :job2Trigger 被执行
 不执行Job

 ------- 关闭Scheduler ---------------------
 ------- 关闭完成 -----------------
 Executed 1 jobs.
 */
public class MyTriggerListener implements TriggerListener{

    private static int count = 0;  // 全局变量
    @Override
    public String getName() {
        return "MyTriggerListener";
    }


    // 当与监听器相关联的 Trigger 被触发，Job 上的 execute() 方法将要被执行时，Scheduler 就调用这个方法。
    @Override
    public void triggerFired(Trigger trigger, JobExecutionContext context) {
        System.out.println("MyTriggerListener :" + context.getTrigger().getKey().getName() + " 被执行");
    }

    /**
     * 在 Trigger 触发后，Job 将要被执行时由 Scheduler 调用这个方法。
     * TriggerListener 给了一个选择去否决 Job 的执行。
     * 假如这个方法返回 true，这个 Job 将不会为此次 Trigger 触发而得到执行。
     */
    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
        if (++count %2 == 0) {
            System.out.println("执行Job");
            return false;
        }
        System.out.println("不执行Job");
        return true;
    }

    /**
     * Scheduler 调用这个方法是在 Trigger 错过触发时。
     * 如这个方法的 JavaDoc 所指出的，你应该关注此方法中持续时间长的逻辑：
     *      在出现许多错过触发的 Trigger 时，
     *      长逻辑会导致骨牌效应。
     *      你应当保持这上方法尽量的小。
     */
    @Override
    public void triggerMisfired(Trigger trigger) {
        System.out.println("Job错过触发");
    }

    /**
     * Trigger 被触发并且完成了 Job 的执行时，Scheduler 调用这个方法。
     * 这不是说这个 Trigger 将不再触发了，而仅仅是当前 Trigger 的触发(并且紧接着的 Job 执行) 结束时。
     * 这个 Trigger 也许还要在将来触发多次的。
     */
    @Override
    public void triggerComplete(Trigger trigger, JobExecutionContext context,CompletedExecutionInstruction triggerInstructionCode) {
        System.out.println("Job执行完毕,Trigger完成");
    }

}
