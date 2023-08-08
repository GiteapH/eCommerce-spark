package org.dataProductor.productor;

import org.dataProductor.utils.csv;
import org.dataProductor.utils.mallActProductorUtils;

import java.util.*;

/**
 * @author Administrator
 * @version 1.0
 * @description: 电商数据生成
 * @date 2023/7/23 13:35
 */
//1.浏览；2.下单；3.关注；4.评论；
//5.加入购物车；6.咨询客服；7.投诉；
public class mallActDataProductor {

    private int[] proportion= new int[7];

    private int single = 0;
    /**
    *@author 吕杨平
    *@Description 按比例生成
    *@Date 14:21 2023/7/23
    *@Param [view, cart, buy, fan, comment, consult, complain]
    *@Return
    */
    public mallActDataProductor(int view,int cart,int buy,int fan,int comment,int consult,int complain,int single){
        this.proportion = new int[]{view, buy, fan, comment, cart,consult, complain};
        this.single = single;
    }


    public String run(Date start,boolean add){
        List<String[]> rows = new ArrayList<>();
        for(int i=0;i<7;i++){
            for(int j=0;j<this.proportion[i]*this.single*(Math.random()+1);j++){
                String[] date_time = mallActProductorUtils.randomDateProductor(start, new Date(start.getTime() + 1000 * 60 * 60 * 23));
                String[] row = new String[] {date_time[0],date_time[1],String.valueOf((int)(Math.random()*1600000)+3000),String.valueOf(i+1),String.valueOf((int)(Math.random()*378457)+1)};
                rows.add(row);
            }
        }
        return csv.toCSVString(rows,add);
    }


    public static void main(String[] args) {
        mallActDataProductor mallActDataProductor = new mallActDataProductor(3,2,1,1,1,1,1,9756);
        System.out.println(mallActProductorUtils.getDateDiff());
    }
}
