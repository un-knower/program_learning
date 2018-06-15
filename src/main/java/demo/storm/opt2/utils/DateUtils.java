package demo.storm.opt2.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
	public static final String 	C_DATE_PATTERN_DEFAULT = "yyyy-MM-dd";
	public static boolean isDate(String createDate,String startDate) {
		SimpleDateFormat format = new SimpleDateFormat(C_DATE_PATTERN_DEFAULT);
		try {
			Date cdate = format.parse(createDate);
			Date sdate = format.parse(createDate);
			return cdate.getTime()>=sdate.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
	}
}
