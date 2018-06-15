package demo.java.time;

import org.quartz.DateBuilder;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Test {
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yy/MM/dd HH:mm");
    static void nextGivenSecondDateTest() throws InterruptedException {
        while (true) {
            Date date = DateBuilder.nextGivenSecondDate(null, 15);  // 当前时间的下一个15s
            System.out.println(date.getTime());
            Thread.sleep(2000);
        }
    }

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

    public static void main(String args[]) throws InterruptedException {

//        nextGivenSecondDateTest();
//        futureDateTest();
        getTimeField();




    }
}
