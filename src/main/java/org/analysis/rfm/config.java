package org.analysis.rfm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

public class config {

    public final static int[] rencencyLayered = new int[] {70,58,42,28,12};

    public final static int[] frequencyLayered = new int[] {1,2,3,4,5};

    public final static int[] consumptionCapacityLayered = new int[] {100,500,3000,8000,12000};

    public final static int[] frequencyLayeredFull = new int[] {2,4,7,9,10};

    public final static int[] consumptionCapacityLayeredFull = new int[] {300, 900, 5800, 15000,20000};



    public float[] getLayeredArray(Dataset<Row> user, Dataset<Row> user_act, Dataset<Row> sku, int time){
        String sql = "SELECT AVG(p) as avgp, AVG( frequency) avgf FROM ( SELECT sum( price ) AS p, user_act.`user`, count( user_act.`user` ) AS frequency, MIN( DATEDIFF( '2018-4-15', act_date )) FROM user_act INNER JOIN `user` ON `user`.id = user_act.`user` INNER JOIN sku ON sku.sku_id = user_act.sku WHERE act_type = 2 GROUP BY user_act.`user` ) AS S";
        user_act.createOrReplaceTempView("user_act");
        user.createOrReplaceTempView("user");
        sku.createOrReplaceTempView("sku");
        Dataset<Row> avgs = user_act.sqlContext().sql(sql);
        float[] ret = new float[2];

        Row first = avgs.first();
        ret[0] = first.getAs("avgp");
        ret[1] = first.getAs("avgf");

        return ret;

    }


//    2018/2/1  -   2018-4-15
    public static String[] getMaxMinDate(Dataset<Row> user_act) {
        String[] ret = new String[2];
        String sql = "SELECT MAX( date ) as maxdate, MIN( date ) as mindate FROM user_act";
        user_act.createOrReplaceTempView("user_act");
        Row first = user_act.sqlContext().sql(sql).first();
        ret[0] = first.getAs("maxdate");
        ret[1] = first.getAs("mindate");
        System.out.println(Arrays.toString(ret));
        return ret;
    }

    public static int judgeRecency(int value){
        for(int i = 0; i < rencencyLayered.length; i++){
            int recency = rencencyLayered[i];
            if(value<=recency){
                return i+1;
            }
        }
        return 5;
    }

    public static int judgeFrequency(int value,int type){
        int[] array;
        switch (type){
            case 1:
                array = frequencyLayered;
                break;
            case 2:
                array = frequencyLayeredFull;
                break;
            default:
                array = new int[] {};
                break;
        }
        for(int i = 0; i < array.length; i++){
            int recency = array[i];
            if(value<=recency){
                return i+1;
            }
        }
        return 5;
    }

    public static int judgeConsumptionCapacity(float value,int type){
        int[] array;
        switch (type){
            case 1:
                array = consumptionCapacityLayered;
                break;
            case 2:
                array = consumptionCapacityLayeredFull;
                break;
            default:
                array = new int[] {};
                break;
        }
        for(int i = 0; i < array.length; i++){
            int recency = array[i];
            if(value<=recency){
                return i+1;
            }
        }
        return 5;
    }

}
