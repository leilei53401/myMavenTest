package date;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
public class CommonDateTools {
    private static Log log = LogFactory.getLog(CommonDateTools.class);
    public static final String LONG_DATE_TIME = "yyyy-MM-dd HH:mm:ss";
    public static final String LONG_DATE_FORMAT = "yyyy-MM-dd";
    public static final String LONG_DATE = "yyyyMMdd";
    public static final String LONG_DATE_HOUR = "yyyyMMddHH";
    public static final String LONG_DATE_MINUTE = "yyyyMMddHHmm";
    public static final String LONG_DATE_SECOND = "yyyyMMddHHmmss";
    public static final String LONG_DATE_TIME_EN = "dd-MMM-yyyy HH:mm:ss.SSS";
    public static final String LONG_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";
    public static final String Long_DATE_TIME_FORMAT_TIME = "yyyy/MM/dd kk:mm:ss";
    
    
    public static void main(String[] args) {
    	
    	//amid=0&hid=0090E60FE65D&sessionid=206544292674597685200000&adposid=15101011&provinceid=10&cityid=1009&channelid=101&programid=112&planid=null&oemid=368&uid=101800638&admt=5&ip=59.44.59.61&
    	//starttime=1481104130&inserttime=1800&stamp=1481102349210
    	//1481105930000
    //starttime=1481104118&inserttime=1800
    	//1481105918000
    	try {
			System.out.println(CommonDateTools.getStrTimeByFormat(1454169600000l,LONG_DATE_TIME));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
    /**
	 * 将long型时间转化为格式化时间(通用)
	 * @param time
	 * @param param
	 * @return
	 * @throws Exception
	 */
    public static String getStrTimeByFormat(long time, String param)
            throws Exception {
        if (time == 0l)
            time = System.currentTimeMillis();
        Date date = new Date(time);
        SimpleDateFormat d = new SimpleDateFormat(param);
        return d.format(date);
    }
    
    
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongTime(String time, String param) {
        if (isNull(time))
            return 0;
        try {
            SimpleDateFormat df = new SimpleDateFormat(param);
            Date date = df.parse(time);
            return date.getTime();
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    
    
    
    /**获得当前时间的前几个月时间
     * 
     * 
     * @return
     */
    public static String getLastMonth(int month) {
        Calendar rightNow = Calendar.getInstance();
        rightNow.add(Calendar.MONTH, -month);
        return new SimpleDateFormat("yyyyMM").format(rightNow.getTime());
        
    }
    
    /**获得当前时间的前三个月时间
     * 
     * 
     * @return
     */
    public static String getLastMonth() {
        Calendar rightNow = Calendar.getInstance();
        rightNow.add(Calendar.MONTH, -3);
        return new SimpleDateFormat("yyyyMM").format(rightNow.getTime());
        
    }
    
    /**
     * 获得业务接入任务的休眠时间
     * 
     * @param upPeriod
     *            ：粒度
     * @param upInterval
     *            ：日期
     * @param getTime
     *            ：时间
     * @return
     */
    public static long getCmccCollectTaskSleep(String period, String interval,
            String time) {
        if (period.toUpperCase().equals("S")) {
            // 如果是分钟报,则返回当前时间到下一个15分钟时刻的时间差
            return getTaskExcuteTimeForMinute(interval);
        } else if (period.toUpperCase().equals("H")) {
            // 如果是小时报,则返回当前时间到下一个整点时刻的时间差
            int min = 0;
            try {
                min = Integer.parseInt(interval);
            } catch (Exception e) {
            }
            return getTaskExcuteTime(min);
        } else if (period.toUpperCase().equals("D")) {
            // 如果是日报,则返回当前时间到下一个执行时间点的时间差
            if (time == null || time.equals("")
                    || time.equalsIgnoreCase("null"))
                return 0l;
            // 计算时分秒
            String[] times = time.split("\\:");
            String hour = "";
            String minute = "";
            String second = "";
            if (times != null && times.length > 2) {
                hour = times[0];
                minute = times[1];
                second = times[2];
                try {
                    int hours = Integer.parseInt(hour);
                    // 上午为0,下午为1
                    int amOrPm = 0;
                    if (hours > 12)
                        amOrPm = 1;
                    return getTaskExcuteTime(hours, Integer.parseInt(minute),
                            Integer.parseInt(second), amOrPm);
                } catch (Exception e) {
                    log.error(e);
                    return 0l;
                }
            } else
                return 0l;
        } else if (period.toUpperCase().equals("M")) {
            // 如果是月报,则返回当前时间到下一个执行时间点的时间差
            if (interval == null || interval.equals("")
                    || interval.equalsIgnoreCase("null"))
                return 0l;
            if (time == null || time.equals("")
                    || time.equalsIgnoreCase("null"))
                return 0l;
            // 计算时分秒
            String[] times = time.split("\\:");
            String hour = "";
            String minute = "";
            String second = "";
            if (times != null && times.length > 2) {
                hour = times[0];
                minute = times[1];
                second = times[2];
                try {
                    int hours = Integer.parseInt(hour);
                    // 上午为0,下午为1
                    int amOrPm = 0;
                    if (hours > 12)
                        amOrPm = 1;
                    return getTaskExcuteTime(interval, hours, Integer
                            .parseInt(minute), Integer.parseInt(second), amOrPm);
                } catch (Exception e) {
                    log.error(e);
                    return 0l;
                }
            } else
                return 0l;
        } else
            return 0l;
    }
    /**
     * 获得从系统启动到下次15分钟时刻的时间(通用)
     * 
     * @param interval
     *            ：间隔时间
     * @return 长整型时间
     */
    public static long getTaskExcuteTimeForMinute(String interval) {
        try {
            return Integer.parseInt(interval) * 60 * 1000;
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 获得从系统启动到下次整点时间的时间差(通用)
     * 
     * @return 长整型时间
     */
    public static long getTaskExcuteTime(int min) {
        try {
            long current = System.currentTimeMillis();// 获得当前时间
            Calendar calendar = Calendar.getInstance();
            int currentYear = calendar.get(Calendar.YEAR);// 获得当前年
            int currentMonth = calendar.get(Calendar.MONTH) + 1;// 获得当前月
            int currentDay = calendar.get(Calendar.DATE);// 获得当前日期
            int currentHour = calendar.get(Calendar.HOUR);// 获得当前小时
            int amOrPm = calendar.get(Calendar.AM_PM);// 上午0或者下午1
            if (amOrPm == 1)
                currentHour = currentHour + 12;
            int currentMin = calendar.get(Calendar.MINUTE);// 获得当前分
            // 获得执行时间
            String currentTime = currentYear + "-" + currentMonth + "-"
                    + currentDay + " " + currentHour + ":" + min + ":00";
            long nextLong = 0l;
            if (currentMin < min) {
                // 获得下一个执行时刻的长整型时间
                nextLong = CommonDateTools.getLongTime(currentTime);
            } else {
                // 获得下一个执行时刻的长整型时间
                nextLong = CommonDateTools.getLongTime(currentTime) + 1 * 60 * 60
                        * 1000;
            }
            return nextLong - current;
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 获得从系统启动到下次任务执行的时间差(通用)
     * 
     * @param hour
     *            ：时
     * @param minute
     *            ：分
     * @param second
     *            ：秒
     * @param amOrPm
     *            ：上午或下午
     * @return 长整型时间
     */
    public static long getTaskExcuteTime(int hour, int minute, int second,
            int amOrPm) {
        try {
            long current = System.currentTimeMillis();// 获得当前时间
            Calendar calendar = Calendar.getInstance();
            int currentYear = calendar.get(Calendar.YEAR);// 获得当前年
            int currentMonth = calendar.get(Calendar.MONTH) + 1;// 获得当前月
            int currentDay = calendar.get(Calendar.DATE);// 获得当前日期
            // 获得当天的1:00的String时间
            String currentExcute = currentYear + "-" + currentMonth + "-"
                    + currentDay + " " + hour + ":" + minute + ":" + second;
            // 获得当天的1:00的long时间
            long currentLong = CommonDateTools.getLongTime(currentExcute);
            // 当前时间与当天1:00的时间差
            long margin = current - currentLong;
            // 执行日期设定为重启日期的第二天
            int excuteDay = currentDay + 1;
            // 如果重启时间比当天1:00早,则执行日期为当天日期
            if (margin <= 0)
                excuteDay = currentDay;
            calendar.set(Calendar.DATE, excuteDay);// 获得执行日期
            // calendar.set(Calendar.DATE, day);// 当天日期
            calendar.set(Calendar.AM_PM, amOrPm);// 上午或者下午
            if (amOrPm == 1)
                hour = hour - 12;
            calendar.set(Calendar.HOUR, hour);// 设定时间
            calendar.set(Calendar.MINUTE, minute);// 设定分钟
            calendar.set(Calendar.SECOND, second);// 设定秒数
            long excute = calendar.getTimeInMillis();// 获得指定时间点的时间
            return excute - current;// 获得当前时间和指定时间点的时间差
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 获得从系统启动到下次任务执行的时间差(通用)
     * 
     * @param interval
     *            ：日期
     * @param hour
     *            ：时
     * @param minute
     *            ：分
     * @param second
     *            ：秒
     * @param amOrPm
     *            ：上午或下午
     * @return 长整型时间
     */
    public static long getTaskExcuteTime(String interval, int hour, int minute,
            int second, int amOrPm) {
        try {
            int day = Integer.parseInt(interval);
            long current = System.currentTimeMillis();// 获得当前时间
            Calendar calendar = Calendar.getInstance();
            int currentYear = calendar.get(Calendar.YEAR);// 获得当前年
            int nextMonth = calendar.get(Calendar.MONTH) + 2;// 获得下一月
            if (nextMonth == 13) {
                currentYear = currentYear + 1;
                nextMonth = 1;
            }
            // 获得下一个执行时间
            String nextTime = currentYear + "-" + nextMonth + "-" + day + " "
                    + hour + ":" + minute + ":" + second;
            log.info("nextTime:" + nextTime);
            // 获得下一个整点时刻的长整型时间
            long nextLong = CommonDateTools.getLongTime(nextTime);
            return nextLong - current;
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 获得前count天的long型时间
     * 
     * @param count
     *            ：前几天
     * @return
     * @throws Exception
     */
    public static long getLongTime(int count) {
        try {
            Calendar calendar = Calendar.getInstance();
            int currentYear = calendar.get(Calendar.YEAR);// 获得当前年
            int currentMonth = calendar.get(Calendar.MONTH) + 1;// 获得当前月
            int currentDay = calendar.get(Calendar.DATE);// 获得当前日期
            // 获得当天0点的String时间
            String currentExcute = currentYear + "-" + currentMonth + "-"
                    + currentDay + " 00:00:00";
            // 获得当天0点的long时间
            long currentLong = CommonDateTools.getLongTime(currentExcute);
            // 获得前count天0点的Long时间
            return currentLong - (count * 24 * 60 * 60 * 1000L);
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongTimeByShortTime(String time) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat(LONG_DATE_FORMAT);
        Date date = df.parse(time);
        return date.getTime();
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongDateByShortTime(String time) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat(LONG_DATE);
        Date date = df.parse(time);
        return date.getTime();
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongDateByYYYYMMDDHH24Time(String time) {
        try {
            SimpleDateFormat df = new SimpleDateFormat(LONG_DATE_HOUR);
            Date date = df.parse(time);
            return date.getTime();
        }catch(Exception e){
            e.printStackTrace();
            return 0 ;
        }
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongDateAndShortTime(String time) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat(LONG_DATE_MINUTE);
        Date date = df.parse(time);
        return date.getTime();
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static String getLongDateAndShortTime(long time) throws Exception {
        Date date = new Date(time);
        SimpleDateFormat d = new SimpleDateFormat(LONG_DATE_MINUTE);
        return d.format(date);
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @param param
     * @return
     * @throws Exception
     */
    public static String getLongDateAndShortTime(long time, String param)
            throws Exception {
        if (time == 0l)
            time = System.currentTimeMillis();
        Date date = new Date(time);
        SimpleDateFormat d = new SimpleDateFormat(param);
        return d.format(date);
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @param param
     * @return
     * @throws Exception
     */
    public static long getLongDateAndShortTime(String time, String param)
            throws Exception {
        SimpleDateFormat df = new SimpleDateFormat(param);
        Date date = df.parse(time);
        return date.getTime();
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongDateByShortTime(String time, String timeParam)
            throws Exception {
        SimpleDateFormat df = new SimpleDateFormat(timeParam);
        Date date = df.parse(time);
        return date.getTime();
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongTime(String time) {
        try {
            SimpleDateFormat df = new SimpleDateFormat(LONG_DATE_TIME);
            Date date = df.parse(time);
            return date.getTime();
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongDateSecondTime(String time) {
        try {
            SimpleDateFormat df = new SimpleDateFormat(
                    Long_DATE_TIME_FORMAT_TIME);
            Date date = df.parse(time);
            return date.getTime();
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 将当前时间转换成long型时间(通用)
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongDateSecond(String time) {
        try {
            SimpleDateFormat df = new SimpleDateFormat(LONG_DATE_SECOND);
            Date date = df.parse(time);
            return date.getTime();
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 将当前时间转换成yyyy-MM-dd HH:mm:ss时间
     * 
     * @param time
     *            ：当前时间
     * @return 2009-02-04 09:58:47
     * @throws Exception
     */
    public static String getDateAndTime(long time) {
        try {
            Date date = new Date(time);
            SimpleDateFormat d = new SimpleDateFormat(LONG_DATE_TIME);
            return d.format(date);
        } catch (Exception e) {
            log.error(e);
            return "";
        }
    }
    /**
     * 将当前时间转换成yyyyMM时间
     * 
     * @param time
     *            ：当前时间
     * @return 2009-02-04 09:58:47
     * @throws Exception
     */
    public static String getDateAndTimeYYYYMM(long time) {
        try {
            Date date = new Date(time);
            SimpleDateFormat d = new SimpleDateFormat("yyyyMM");
            return d.format(date);
        } catch (Exception e) {
            log.error(e);
            return "";
        }
    }
    
    /**
     * 将当前时间转换成DD时间
     * 
     * @param time
     *            ：当前时间
     * @return 2009-02-04 09:58:47
     * @throws Exception
     */
    public static String getDateAndTimeDD(long time) {
        try {
            Date date = new Date(time);
            SimpleDateFormat d = new SimpleDateFormat("dd");
            return d.format(date);
        } catch (Exception e) {
            log.error(e);
            return "";
        }
    }
    
    /**
     * 将当前时间转换成long型时间
     * 
     * @param time
     *            ：当前时间
     * @return
     * @throws Exception
     */
    public static long getLongDateTimeEn(String time) {
        try {
            SimpleDateFormat df = new SimpleDateFormat(LONG_DATE_TIME_EN,
                    Locale.ENGLISH);
            Date date = df.parse(time);
            return date.getTime();
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 将当前时间转换成dd-MMM-yyyy HH:mm:ss:SSS时间
     * 
     * @param time
     *            ：当前时间
     * @return 06-Dec-2009 22:43:38:515
     * @throws Exception
     */
    public static String getLongDateTimeEn(long time) {
        try {
            Date date = new Date(time);
            SimpleDateFormat d = new SimpleDateFormat(LONG_DATE_TIME_EN,
                    Locale.ENGLISH);
            return d.format(date);
        } catch (Exception e) {
            log.error(e);
            return "";
        }
    }
    /**
     * 将当前时间转换成符合param格式的时间
     * 
     * @param param
     *            ：格式
     * @return
     * @throws Exception
     */
    public static String getDate(String param, int count) {
        try {
            long time = getLongTime(count);
            Date date = new Date(time);
            SimpleDateFormat d = new SimpleDateFormat(param);
            return d.format(date);
        } catch (Exception e) {
            log.error(e);
            return "";
        }
    }
    /**
     * 将当前时间转换成yyyy-MM-dd时间
     * 
     * @param period
     *            ：获得数据的粒度
     * @param interval
     *            ：获得数据的日期
     * @param time
     *            ：获得数据的时间
     * @param param
     *            ：时间格式参数
     * @param count
     *            ：前几天
     * @param min
     *            ：结束时间减1秒
     * @return 2009-02-04
     * @throws Exception
     */
    public static String getLongDate(String period, String interval,
            String time, String format, int count, long min) {
        String returnTime = "%";
        try {
            if (isNull(period))
                return returnTime;
            else if (period.equals("H")) {
                // 如果是小时粒度
            } else if (period.equals("D")) {
                // 如果是天粒度
                if (format.trim().toLowerCase().indexOf("long") != -1) {
                    // 如果参数是长整型格式,截取格式
                    String[] param = format.split("\\_");
                    int d = 13;
                    if (param != null && param.length > 1)
                        // 获得几位长整型
                        d = Integer.parseInt(param[1]);
                    // 获得13位长整型时间,则返回毫秒值
                    long temp = getLongTime(count);
                    returnTime = temp + "";
                    // 如果是10位长整型时间,则返回秒值
                    if (d == 10)
                        returnTime = (temp / 1000) + "";
                } else {
                    // 如果参数是日期格式
                    long temp = getLongTime(count);
                    Date date = new Date(temp - min);
                    SimpleDateFormat d = new SimpleDateFormat(format);
                    returnTime = d.format(date);
                }
            } else if (period.equals("M")) {
                // 如果是月粒度
            }
        } catch (Exception e) {
            log.error(e);
        }
        return returnTime;
    }
    /**
     * 将当前时间转换成yyyy-MM-dd时间
     * 
     * @param time
     *            ：时间
     * @param param
     *            ：加多少毫秒
     * @return 2009-02-04
     * @throws Exception
     */
    public static String getLongDate(String time, long param) {
        try {
            Date date = new Date(Long.parseLong(time) + param);
            SimpleDateFormat d = new SimpleDateFormat(LONG_DATE_TIME);
            return d.format(date);
        } catch (Exception e) {
            return "";
        }
    }
    /**
     * 判断字符串是否为空
     * 
     * @param param
     * @return
     */
    public static boolean isNull(String param) {
        if (param == null || param.equals("") || param.equalsIgnoreCase("null"))
            return true;
        else
            return false;
    }
    /**
     * 等待的时间
     * 
     * @param time
     * @return
     */
    public static String getSleepTime(long time) {
        int day = (int) time / 1000 / 60 / 60 / 24;
        int hour = (int) (time - day * 24 * 60 * 60 * 1000) / 1000 / 60 / 60;
        int minute = (int) (time - day * 24 * 60 * 60 * 1000 - hour * 60 * 60 * 1000) / 1000 / 60;
        int second = (int) (time - day * 24 * 60 * 60 * 1000 - hour * 60 * 60
                * 1000 - minute * 60 * 1000) / 1000;
        return "等候：" + day + "天," + hour + "小时," + minute + "分钟," + second
                + "秒！";
    }
    /**
     * 获得上一个15分钟的开始时间
     * 
     * @param current
     * @param min
     * @return
     */
    public static long getBeginTimeForFifteen(long current, int min) {
        try {
            int year = 0;
            int month = 1;
            int day = 1;
            int minute = 0;
            int hour = 0;
            Calendar calendar = Calendar.getInstance();
            if (current == 0) {
                year = calendar.get(Calendar.YEAR);
                month = calendar.get(Calendar.MONTH + 1);
                day = calendar.get(Calendar.DATE);
                hour = calendar.get(Calendar.HOUR);
                minute = calendar.get(Calendar.MINUTE);
            } else {
                Date date = new Date(current);
                year = date.getYear() + 1900;
                month = date.getMonth() + 1;
                day = date.getDate();
                hour = date.getHours();
                minute = date.getMinutes();
            }
            int newMinute = 0;
            if (minute >= 45)
                newMinute = 45 - min;
            else if (minute >= 30)
                newMinute = 30 - min;
            else if (minute >= 15)
                newMinute = 15 - min;
            else {
                hour = hour - 1;
                newMinute = 60 - min;
            }
            String currentTime = year + "-" + month + "-" + day + " " + hour
                    + ":" + newMinute + ":00";
            return CommonDateTools.getLongTime(currentTime);
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 获得上一个5分钟的开始时间
     * 
     * @param current
     * @param min
     * @return
     */
    public static long getBeginTimeForFive(long current, int min) {
        try {
            int year = 0;
            int month = 1;
            int day = 1;
            int minute = 0;
            int hour = 0;
            Calendar calendar = Calendar.getInstance();
            if (current == 0) {
                year = calendar.get(Calendar.YEAR);
                month = calendar.get(Calendar.MONTH + 1);
                day = calendar.get(Calendar.DATE);
                hour = calendar.get(Calendar.HOUR);
                minute = calendar.get(Calendar.MINUTE);
            } else {
                Date date = new Date(current);
                year = date.getYear() + 1900;
                month = date.getMonth() + 1;
                day = date.getDate();
                hour = date.getHours();
                minute = date.getMinutes();
            }
            int newMinute = 0;
            if (minute >= 55)
                newMinute = 55 - min;
            else if (minute >= 50)
                newMinute = 50 - min;
            else if (minute >= 45)
                newMinute = 45 - min;
            else if (minute >= 40)
                newMinute = 40 - min;
            else if (minute >= 35)
                newMinute = 35 - min;
            else if (minute >= 30)
                newMinute = 30 - min;
            else if (minute >= 25)
                newMinute = 25 - min;
            else if (minute >= 20)
                newMinute = 20 - min;
            else if (minute >= 15)
                newMinute = 15 - min;
            else if (minute >= 10)
                newMinute = 10 - min;
            else if (minute >= 5)
                newMinute = 5 - min;
            else {
                hour = hour - 1;
                newMinute = 60 - min;
            }
            String currentTime = year + "-" + month + "-" + day + " " + hour
                    + ":" + newMinute + ":00";
            return CommonDateTools.getLongTime(currentTime);
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 获得上一个10分钟的开始时间
     * 
     * @param current
     * @param min
     * @return
     */
    public static long getBeginTimeForTen(long current, int min) {
        try {
            int year = 0;
            int month = 1;
            int day = 1;
            int minute = 0;
            int hour = 0;
            Calendar calendar = Calendar.getInstance();
            if (current == 0) {
                year = calendar.get(Calendar.YEAR);
                month = calendar.get(Calendar.MONTH + 1);
                day = calendar.get(Calendar.DATE);
                hour = calendar.get(Calendar.HOUR);
                minute = calendar.get(Calendar.MINUTE);
            } else {
                Date date = new Date(current);
                year = date.getYear() + 1900;
                month = date.getMonth() + 1;
                day = date.getDate();
                hour = date.getHours();
                minute = date.getMinutes();
            }
            int newMinute = 0;
            if (minute >= 50)
                newMinute = 50 - min;
            else if (minute >= 40)
                newMinute = 40 - min;
            else if (minute >= 30)
                newMinute = 30 - min;
            else if (minute >= 20)
                newMinute = 20 - min;
            else if (minute >= 10)
                newMinute = 10 - min;
            else {
                hour = hour - 1;
                newMinute = 60 - min;
            }
            String currentTime = year + "-" + month + "-" + day + " " + hour
                    + ":" + newMinute + ":00";
            return CommonDateTools.getLongTime(currentTime);
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 获得上一个10分钟的结束时间
     * 
     * @param current
     * @param min
     * @return
     */
    public static long getEndTen(long current, int min) {
        return getBeginTimeForTen(current, min) + 10 * 60 * 1000;
    }
    
    /**
     * 获得上一个小时的开始时间
     * 
     * @param current
     * @return
     */
    public static long getBeginTimeForHour(long current) {
        try {
            int year = 0;
            int month = 1;
            int day = 1;
            int hour = 0;
            Calendar calendar = Calendar.getInstance();
            if (current == 0) {
                year = calendar.get(Calendar.YEAR);
                month = calendar.get(Calendar.MONTH + 1);
                day = calendar.get(Calendar.DATE);
                hour = calendar.get(Calendar.HOUR);
            } else {
                Date date = new Date(current);
                year = date.getYear() + 1900;
                month = date.getMonth() + 1;
                day = date.getDate();
                hour = date.getHours();
            }
            String currentTime = year + "-" + month + "-" + day + " "
                    + (hour - 1) + ":00:00";
            return CommonDateTools.getLongTime(currentTime);
        } catch (Exception e) {
            log.error(e);
            return 0l;
        }
    }
    /**
     * 获得上一个15分钟的结束时间
     * 
     * @param current
     * @param min
     * @return
     */
    public static long getEndTime(long current, int min) {
        return getBeginTimeForFifteen(current, min) + 15 * 60 * 1000;
    }
    /**
     * 获得上一个5分钟的结束时间
     * 
     * @param current
     * @param min
     * @return
     */
    public static long getEndTimeForFive(long current, int min) {
        return getBeginTimeForFive(current, min) + 5 * 60 * 1000;
    }
    /**
     * 获得文件大小
     * 
     * @param file
     * @return
     */
    public static double getFileSize(File file) {
        if (file == null || !file.exists()) {
            log.error("file == null || !file.exists()");
            return 0.00;
        }
        FileInputStream input = null;
        int fileSize = 0;
        try {
            try {
                input = new FileInputStream(file);
            } catch (FileNotFoundException fe) {
                log.error("FileNotFoundException:", fe);
                return 0.00;
            }
            if (input == null) {
                log.error("input == null");
                return 0.00;
            }
            try {
                fileSize = input.available();
            } catch (IOException ie) {
                log.error("IOException:", ie);
                return 0.00;
            }
            double f = (double) fileSize;
            DecimalFormat df = new DecimalFormat("##.00");
            return Double.parseDouble(df.format(f / 1024 / 1024));
        } catch (Exception e) {
            log.error("Exception:", e);
            return 0.00;
        }
    }
    /**
     * 获得文件大小(返回字节数)
     * 
     * @param file
     * @return
     */
    public static double getFileSizeForByte(File file) {
        if (file == null || !file.exists()) {
            log.error("file == null || !file.exists()");
            return 0.00;
        }
        FileInputStream input = null;
        int fileSize = 0;
        try {
            try {
                input = new FileInputStream(file);
            } catch (FileNotFoundException fe) {
                log.error("FileNotFoundException:", fe);
                return 0.00;
            }
            if (input == null) {
                log.error("input == null");
                return 0.00;
            }
            try {
                fileSize = input.available();
            } catch (IOException ie) {
                log.error("IOException:", ie);
                return 0.00;
            }
            double f = (double) fileSize;
            // DecimalFormat df = new DecimalFormat("##.00");
            // return Double.parseDouble(df.format(f / 1024 / 1024));
            return f;
        } catch (Exception e) {
            log.error("Exception:", e);
            return 0.00;
        }
    }
    /**
     * 获得起止时间相差个数
     * 
     * @param perTime
     *            ：任务上一次执行时间
     * @param start
     *            ：任务本次执行时间
     * @param min
     *            ：分钟
     * @return
     */
    public static int getTableCount(long perTime, long start, int min) {
        if (perTime == 0l || start == 0l)
            return 0;
        long count = 0;
        try {
            count = (start - perTime) / 1000 / 60 / min;
        } catch (Exception e) {
            log.error(e);
        }
        return (int) count;
    }
    /**
     * 获得时间参数
     * 
     * @param time
     * @param param
     * @return
     */
    public static String getTime(long time, String param) {
        if (isNull(param))
            return String.valueOf(time);
        try {
            Date date = new Date(time);
            SimpleDateFormat d = new SimpleDateFormat(param);
            return d.format(date);
        } catch (Exception e) {
            return String.valueOf(time);
        }
    }
    
    /**
     * 获得时间参数
     * 
     * @param time
     * @param param
     * @return
     */
    public static long getLongTime(long time, String param) {
        if (isNull(param))
            return time;
        try {
            Date date = new Date(time);
            SimpleDateFormat d = new SimpleDateFormat(param);
            return getLongTime(d.format(date), LONG_DATE_SECOND);
        } catch (Exception e) {
            return time;
        }
    }
    public static String getNewTimeLastTime(Date newTime) {
        String lastTime = null;
        try {
            String simple = new SimpleDateFormat("HHmmss").format(newTime);
            int subtime = Integer.parseInt(simple.substring(0, 2));
            --subtime;
            Calendar cal = Calendar.getInstance();
            cal.set(11, subtime);
            lastTime = new SimpleDateFormat("HHmmss").format(cal.getTime());
            return lastTime;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lastTime;
    }
    
    /**
     * 解析华为文件中的时间，转换为long类型
     * 
     * @param time
     * 
     * @return
     */
    public static long getHuaweiTime(String time) {
        long timeLong=0l;
        try {
            SimpleDateFormat dateFormat = new java.text.SimpleDateFormat(
                    "yyyy-MM-dd'T'HH:mm:ss'+08:00'");
            Date dateTime = dateFormat.parse(time);
            timeLong = dateTime.getTime();
        } catch (ParseException e) {
            log.error("dttime时间转换错误");
            timeLong = 0;
        }
        return timeLong;
    }
    
    /**
     * 将当前时间转换成时间
     * 
     * @param time
     *            ：当前时间
     * @return 2009-02-04 09:58:47
     * @throws Exception
     */
    public static String getDateAndTimeDD(long time,String type) {
        try {
            Date date = new Date(time);
            SimpleDateFormat d = new SimpleDateFormat(type);
            return d.format(date);
        } catch (Exception e) {
            log.error(e);
            return "";
        }
    }
}
