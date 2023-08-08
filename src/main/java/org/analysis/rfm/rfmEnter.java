package org.analysis.rfm;

import org.analysis.Enum.SpliteDateEnum;
import org.analysis.rfm.udf.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import java.util.HashMap;
import java.util.Map;
import java.io.Serializable;
public class rfmEnter implements Serializable{
    static   Map<String, Double> avgs;

    public Dataset<Row> rfm(Dataset<Row> user, Dataset<Row> user_act, Dataset<Row> sku, int time) {
//

        user_act.createOrReplaceTempView("user_act");
        user.createOrReplaceTempView("user");
        sku.createOrReplaceTempView("sku");
        user_act.sqlContext().udf().register("judegeLayeredLongUDF", new judegeLayeredLongUDF(), DataTypes.IntegerType);
        user_act.sqlContext().udf().register("judegeLayeredFloatUDF", new judegeLayeredFloatUDF(), DataTypes.IntegerType);
        user_act.sqlContext().udf().register("judegeLayeredIntUDF", (Integer value, Integer type) -> config.judgeRecency(value), DataTypes.IntegerType);
        String sql = "";
        sql = String.format("SELECT user_act.`user`,first(address) as address, judegeLayeredLongUDF ( count( user_act.`user` ), %d ) AS frequency_score, judegeLayeredIntUDF ( MIN(datediff('2018-04-15',act_date)), %d ) AS recency_score, judegeLayeredFloatUDF ( sum( price ), %d ) AS consumption_capacity_score, sum( price ) AS consumption_capacity, count( user_act.`user` ) AS frequency, MIN( DATEDIFF( '2018-04-15', act_date)) AS recency FROM user_act INNER JOIN `user` ON `user`.id = user_act.`user` INNER JOIN sku ON sku.sku_id = user_act.sku WHERE act_type = 2 %s GROUP BY user_act.`user`",time,time,time, SpliteDateEnum.getInstance(time).getSql(true   ));
        Dataset<Row> rfmDataset = user_act.sqlContext().sql(sql);
        return rfmDataset;
    }

    public Dataset<Row> analysisRfmTag(Dataset<Row> rfm){
        avgs = analysisAvg(rfm);
        rfm.sqlContext().udf().register("to_tag", (Integer value, String avgField) -> {
            if(avgs.get(avgField)>value){
                return "低";
            }else{
                return "高";
            }
        },DataTypes.StringType);
        rfm.sqlContext().udf().register("to_user_type", new toUserTypeUDF(),DataTypes.StringType);
       rfm.createOrReplaceTempView("rfm");
       return rfm.sqlContext().sql("select user,address as rfm_address,to_tag(frequency_score,'frequency_score') as frequency_score_tag,to_tag(recency_score,'recency_score') as recency_score_tag,to_tag(consumption_capacity_score,'consumption_capacity_score') as consumption_capacity_score_tag," +
               "to_user_type(to_tag(frequency_score,'frequency_score'),to_tag(recency_score,'recency_score'),to_tag(consumption_capacity_score,'consumption_capacity_score')) as rfm_tag,frequency_score,recency_score,consumption_capacity_score,consumption_capacity,frequency,recency from rfm");
    }

    private Map<String,Double> analysisAvg(Dataset<Row> rfm){
        Map<String,Double> res = new HashMap<>();
        rfm.createOrReplaceTempView("rfm");
        Dataset<Row> avgDf = rfm.sqlContext().sql("select avg(frequency_score) as frequency_score_avg,avg(recency_score) as recency_score_avg,avg(consumption_capacity_score) from rfm");
        avgDf.show();
        Row first = avgDf.first();
        res.put("frequency_score",first.getDouble(0));
        res.put("recency_score", first.getDouble(1));
        res.put("consumption_capacity_score", first.getDouble(2));
        return res;
    }
}
