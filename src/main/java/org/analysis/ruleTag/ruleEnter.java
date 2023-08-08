package org.analysis.ruleTag;

import org.analysis.Enum.SplitPriceDataEnum;
import org.analysis.Enum.SplitTimeDataEnum;
import org.analysis.Enum.SpliteDateEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class ruleEnter implements Serializable {
//    分段偏好

    public Dataset<Row> analysisSplitter(Dataset<Row> user,Dataset<Row> user_act,Dataset<Row> sku,int time){

        //    分析价格分段 ， 时间分段
        //    建临时视图
        user.createOrReplaceTempView("user");
        user_act.createOrReplaceTempView("user_act");
        sku.createOrReplaceTempView("sku");
        String sql = String.format("SELECT id,first(address) as address, sum(IF(%s,1,0)) AS dawn, sum(IF(%s,1,0)) AS morning, sum(IF(%s,1,0)) AS noon, sum(IF(%s,1,0)) as midday, sum(IF(%s,1,0)) as afternoon, sum(IF(%s,1,0)) as evening,SUM(IF(%s,1,0)) as low,SUM(IF(%s,1,0)) as super_low,SUM(IF(%s,1,0)) as medium,SUM(IF(%s,1,0)) as above_moderate,SUM(IF(%s,1,0)) as finest,SUM(IF(%s,1,0)) as higher,SUM(IF(%s,1,0)) as highest FROM `user` LEFT JOIN user_act ON `user`.id = user_act.`user` INNER JOIN sku ON sku.sku_id = user_act.sku where true %s GROUP BY id ",
                SplitTimeDataEnum.DAWN.getSql(false), SplitTimeDataEnum.MORNGING.getSql(false), SplitTimeDataEnum.NOON.getSql(false), SplitTimeDataEnum.MIDDAY.getSql(false),SplitTimeDataEnum.AFTERNOON.getSql(false),SplitTimeDataEnum.EVENING.getSql(false),
                SplitPriceDataEnum.Low.getSql(false),SplitPriceDataEnum.SuperLow.getSql(false),SplitPriceDataEnum.MEDIUM.getSql(false),SplitPriceDataEnum.ABOVEMODERATE.getSql(false),SplitPriceDataEnum.FINEST.getSql(false),SplitPriceDataEnum.HIGHER.getSql(false),SplitPriceDataEnum.HIGHIEST.getSql(false),
                SpliteDateEnum.getInstance(time).getSql(true));
//        String sql = "select act_time, IF(  act_time between '18:00:00' AND '23:59:59' ,1,0) from user_act where user = 463";
        System.err.println(sql);
        return user.sqlContext().sql(sql);
    }

    public Dataset<Row> analysisCumulative(Dataset<Row> user,Dataset<Row> user_act,int time){
        user.createOrReplaceTempView("user");
        user_act.createOrReplaceTempView("user_act");
        String sql = String.format("SELECT act_type,id,count(act_type) as count FROM `user` LEFT JOIN user_act ON `user`.id = user_act.`user` where true %s GROUP BY id,act_type",SpliteDateEnum.getInstance(time).getSql(true));
        return user.sqlContext().sql(sql);
    }
}
