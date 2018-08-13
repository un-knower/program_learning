package demo.java.time;

import org.quartz.DateBuilder;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Test {
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yy/MM/dd HH:mm");
    static SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 当前时间经过时间n后的时间
     * @throws InterruptedException
     */
    static void nextGivenSecondDateTest() throws InterruptedException {
        while (true) {
            Date date = DateBuilder.nextGivenSecondDate(null, 15);  // 当前时间的下一个15s
            System.out.println(date.getTime());
            Thread.sleep(2000);
        }
    }

    /**
     * 当前时间经过时间n后的时间
     * @throws InterruptedException
     */
    static void futureDateTest() {
        Date date = new Date();
        System.out.println(simpleDateFormat.format(date));
        Date futureDate = DateBuilder.futureDate(-1, DateBuilder.IntervalUnit.HOUR);
        System.out.println(simpleDateFormat.format(futureDate));


        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.HOUR, -1);
        System.out.println(simpleDateFormat.format(calendar.getTime()));

    }

    static void getTimeField() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());

        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DATE);
        int hour12 = calendar.get(Calendar.HOUR);  // 12小时制
        int hour24 = calendar.get(Calendar.HOUR_OF_DAY);  // 24小时制
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        int millisecond = calendar.get(Calendar.MILLISECOND);

        System.out.println(year);
        System.out.println(month+1);
        System.out.println(day);
        System.out.println(hour24);
        System.out.println(minute);
        System.out.println(second);
        System.out.println(millisecond);

    }

    /**
     * 获取当前时间到第二天凌晨的秒数
     */
    static void getSecondsNextEarlyMorning() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, 1);
        // 改成这样就好了
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Long seconds = (cal.getTimeInMillis() - System.currentTimeMillis()) / 1000;
        System.out.println(seconds);
    }

    /**
     * 第二天的某个时间
     */
    static void getTomorrowTime() {
        Date date = DateBuilder.tomorrowAt(6, 0, 0);
        System.out.println(simpleDateFormat2.format(date));
    }

    static void getMillisSeconds2NextTime() {
        // 例如当前时间距离下个06:00:00还有多少毫秒
        Date date = DateBuilder.tomorrowAt(6, 0, 0);
        System.out.println("next time="+date.getTime()+", "+simpleDateFormat2.format(date));
        long currentTime = System.currentTimeMillis();
        System.out.println("current time="+currentTime+", "+simpleDateFormat2.format(new Date(currentTime)));
        long l = date.getTime() - currentTime;
        System.out.println("相距="+l);
        // 但是如果是 2018-08-03 01:00:00，那么下个06:00:00应该是2018-08-03 06:00:00，而不应该是2018-08-04 06:00:00
        // 所以需要继续判断
        if (l >= 60*60*24*1000) {
            l = l-60*60*24*1000;
            System.out.println("重新计算相距="+l);
        }


    }

    public static void main(String args[]) throws InterruptedException {

//        nextGivenSecondDateTest();
//        futureDateTest();
//        getTimeField();
//        getSecondsNextEarlyMorning();
        getMillisSeconds2NextTime();




    }
}
