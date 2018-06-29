package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * @author bjwangguangliang
 * Date: 17-11-24
 * Time: 上午11:13
 * 解析时间/时间戳 得到 drui时间 yyyy-MM-ddTHH:mm:ss:SSS
 */
public class TimeUtil {
    //private static final String DRUID_TIMEFORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'";
    //private static final String DRUID_TIMEFORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String DRUID_TIMEFORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final SimpleDateFormat druidDateFormat = new SimpleDateFormat(DRUID_TIMEFORMAT);
//    private static SimpleDateFormat sourceTimeFormat = null;
//    private static SimpleDateFormat timeFormat = null;
    private static final long TIME_STAMP_NUM = 1000000000000L;
    private static final Calendar cal = Calendar.getInstance();
    /**
     * 将时间格式化为druid格式的时间：2017-11-18T13:26:49.667+08:00
     *
     * @param time
     * @param format
     *      年： yyyy
     *      月： MM
     *      日： dd
     *      时： HH
     *      分： mm
     *      秒： ss
     *      毫秒：SSS
     *
     * @return druid格式的时间
     */
    public static String timeFormat2DruidTime(String time, String format) {
        SimpleDateFormat sourceTimeFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = sourceTimeFormat.parse(time);
            cal.setTime(date);
            cal.add(Calendar.HOUR, -8);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return druidDateFormat.format(cal.getTime());
    }


    /**
     * 将时间戳格式化为druid格式的时间：2017-11-18T13:26:49.667+08:00
     *
     * @param timestamp 可以不带毫秒(例如：1511513037)或者带毫秒(例如：1511513037456l)
     * @return druid格式的时间
     */
    public static String timestampFormat2DruidTime(long timestamp) {
        Date date = null;
        //毫秒级
        if(timestamp / TIME_STAMP_NUM != 0) {
            date = new Date(timestamp);
        } else { //秒级
            date = new Date(timestamp*1000);
        }
        cal.setTime(date);
        cal.add(Calendar.HOUR, -8);
        return druidDateFormat.format(cal.getTime());
    }

    public static long timeFormat2TimeStamp(String time, String format) {
        SimpleDateFormat sourceTimeFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = sourceTimeFormat.parse(time);
            return date.getTime();
        } catch (ParseException e) {
            return 0L;
        }
    }

    public static long timeFormat2TimeStamp(String time) throws ParseException {
        SimpleDateFormat sourceTimeFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = null;
            date = sourceTimeFormat.parse(time);
            return date.getTime();
    }

    public static long timeFormat2TimeStamp(long timestamp) {
        //毫秒级
        if(timestamp / TIME_STAMP_NUM != 0) {
            return timestamp;
        } else { //秒级
            return timestamp*1000;
        }
    }



    public static String timeFormat(String time, String sourceFormat, String format) {
        SimpleDateFormat sourceTimeFormat = new SimpleDateFormat(sourceFormat);
        SimpleDateFormat timeFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = sourceTimeFormat.parse(time);
            return timeFormat.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }

    public static long now() {
        return System.currentTimeMillis();
    }

}
