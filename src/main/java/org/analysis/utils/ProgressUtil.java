package org.analysis.utils;

import org.apache.log4j.Logger;
import java.io.Serializable;
import java.util.*;

/***
 * 进度工具
 * @author xpl
 *
 */
public class ProgressUtil implements Serializable{
    private static final Logger logger = Logger.getLogger("");
    // 工作内容
    String title;
    // 工作量化后总数量
    long total;
    // 已完成的数量
    int finish = 0;
    long startTime;
    long preTime;
    long nextTime;
    //完成一项工作后是否自动休眠
    boolean autoSleep = false;
    //上一次休眠时长
    long preSleepTime;
    //休眠耗时。参考sleep()方法注释
    long [] timeArr = {8000,3000,2000,1000,4000,5000,6000,7000,8000};
    //完成指定数量。参考sleep()方法注释
    int [] countArr = {1,10,20,40,60,80,100};

    public ProgressUtil(String title, long total) {
        this.title = title;
        this.total = total;
        startTime = System.currentTimeMillis();
        preTime = startTime;
    }
    public ProgressUtil(String title, int total, boolean autoSleep) {
        this(title, total);
        this.autoSleep = autoSleep;
    }

    /***
     * 失败一次，打印信息。
     */
    public void failureOne(String errorMsg) {
        logger.error(title + " | " + total + " / " + finish  + " | " + "休眠时长："+ preSleepTime +"|错误信息："+errorMsg);
    }

    /***
     * 完成一个任务后，打印信息
     */
    public void finishOne() {
        finishOne("");
    }

    /***
     * 完成一个任务后，打印信息
     * @param taskInfo
     *            任务信息
     */
    public void finishOne(String taskInfo) {
        finish++;
        long sleepTime = sleep();
        nextTime = System.currentTimeMillis();
        String str = title + " | 耗时" + getHs(nextTime - preTime) + " | " + total + " / " + finish;
        str += " | 剩余时间:" + getHs(remainingTime());
        str += " | " + taskInfo;
        if(finish == total){
            str += " | 总耗时" + getHs(nextTime - startTime);
        }
        if(sleepTime > 0){
            str += " | 休眠" + getHs(sleepTime);
        }
        System.err.print("\r"+str);
//		System.out.println(str);
        preTime = nextTime;

    }

    /***
     * 耗时
     * @param
     * @return
     */
    public String getHs(long hs) {
        if (hs < 1000) {
            return hs + "毫秒";
        } else if (hs < 1000 * 60) {
            return (hs / 1000) + "秒";
        } else if (hs < 1000 * 60 * 60) {
            return (hs / (1000 * 60)) + "分" + ((hs % (1000 * 60)) / 1000) + "秒";
        } else if (hs < 1000 * 60 * 60 * 60) {
            return (hs / (1000 * 60 * 60)) + "时" + ((hs % (1000 * 60 * 60)) / (1000 * 60)) + "分";
        }else if(hs < 1000L * 60 * 60 * 60 * 24){
            return (hs / (1000 * 60 * 60 * 24)) + "天" + (hs % (1000 * 60 * 60 * 24)) / (1000*60*60)+"时" ;
        }
        return hs + "毫秒";
    }

    /***
     * 自动休眠，autoSleep为true时开启。
     * 默认：
     * long [] timeArr = {0,1000,2000,3000,4000,5000,10000};
     * int [] countArr = {1,3,5,10,20,50,100};
     * 含义：完成数finish小于等于countArr[i]时，休眠timeArr[i]时长。
     * finish 大于countArr[countArr.length - 1]时，休眠timeArr[timeArr.length - 1]时长。
     */
    private long sleep(){
        preSleepTime = 0;
        if(autoSleep && finish < total){
            int i = 0;
            for (; i < countArr.length; i++) {
                if(finish <= countArr[i]){
                    break;
                }
            }
            try {
                if(i < timeArr.length){
                    preSleepTime = timeArr[i];
                }else{
                    preSleepTime = timeArr[timeArr.length - 1];
                }
                Thread.sleep(preSleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return preSleepTime;
    }
    /***
     * 估计剩余时间
     * 先根据已完成任务数量，求完成一次的平均时间，然后根据待完成任务数估计完成所有任务剩余时间。
     * @return
     */
    public long remainingTime() {
        return (nextTime - startTime)/finish * (total - finish);
    }

}