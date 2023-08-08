package org.dataProductor.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author Administrator
 * @version 1.0
 * @description: 数据生成工具类
 * @date 2023/7/23 17:20
 */
public class mallActProductorUtils {
    /*
    生成固定范围内随机时间
     */
    public static String[] randomDateProductor(Date start,Date end){
        long diff = end.getTime()- start.getTime();
        long randomLong = (long)(Math.random() * diff) + 1;
        Date date = new Date(start.getTime()+randomLong);
        String dateStr =  new SimpleDateFormat("yyyy/MM/dd").format(date);
        String timeStr =  new SimpleDateFormat("HH:mm:ss").format(date);
        return new String[] {dateStr,timeStr};
    }

    public static int getDateDiff(){
        Calendar calendar1 = Calendar.getInstance();
        calendar1.set(2018,Calendar.APRIL,15);
        long num=new Date().getTime()-calendar1.getTime().getTime();//时间戳相差的毫秒数
        return (int) (num/24/60/60/1000);
    }

    public static String[] calcurlateDateDiff(Calendar start){
        long timeLag = new Date().getTime()-start.getTime().getTime();
        long day=timeLag/(24*60*60*1000);
        //小时
        long hour=(timeLag/(60*60*1000)-day*24);
        //分钟
        long minute=((timeLag/(60*1000))-day*24*60-hour*60);
        //秒
        long second=(timeLag/1000-day*24*60*60-hour*60*60-minute*60);
        return new String[]{String.format("%d天%d小时%d分钟%d秒",day,hour,minute,second),String.valueOf(timeLag/1000)};
    }

    public static Date calcurlateDate(Calendar calendar,int second){
        Calendar t = (Calendar) calendar.clone();

        t.add(Calendar.SECOND,second);

        return t.getTime();
    }
}
