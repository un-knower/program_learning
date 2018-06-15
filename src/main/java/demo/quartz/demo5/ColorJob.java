package demo.quartz.demo5;

import org.quartz.*;

import java.text.SimpleDateFormat;
import java.util.Date;

//对jobDateMap实现持久化   将上次处理过得值存入jobDateMap
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class ColorJob implements Job{
    public static final String FAVORITE_COLOR = "favorite color";
    public static final String EXECUTION_COUNT = "count";
    private int _counter = 1;


    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
        JobKey jobKey = context.getJobDetail().getKey();
        JobDataMap data = context.getJobDetail().getJobDataMap();

        String favoriteColor = data.getString("color");
        int count = data.getInt("count");
        System.out.println(("ColorJob:  在 " + dateFormat.format(new Date()) + "执行  "+  jobKey +"\n"
                + " color : " + favoriteColor + "\n"
                + " 第  " + count + "次 执行\n"
                + " 成员变量_counter是第 " + this._counter+ "次 执行"));

        ++ count;
        data.put("count", count);
        this._counter += 1;

    }
}
