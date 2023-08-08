package org.analysis.ruleTag;

import org.analysis.ruleTag.udf.getPriceTagByValueUDF;
import org.analysis.ruleTag.udf.getTimeTagByValueUDF;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.types.DataTypes;

public class groupRule{
    public static Dataset<Row> group(Dataset<Row> spliter){
        spliter.createOrReplaceTempView("spliter");
        SQLContext sqlContext = spliter.sqlContext();
        sqlContext.udf().register("getTimeTagByNum",new getTimeTagByValueUDF(), DataTypes.StringType);
        sqlContext.udf().register("getPriceTagByNum",new getPriceTagByValueUDF(),DataTypes.StringType);
       return  sqlContext.sql("select id,address,GREATEST(dawn,morning,noon,midday,afternoon,evening) as max_time,GREATEST(low,super_low,above_moderate,finest,higher,highest) as max_price,getTimeTagByNum(dawn,morning,noon,midday,afternoon,evening,GREATEST(dawn,morning,noon,midday,afternoon,evening)) as max_time_num,getPriceTagByNum(low,super_low,medium,above_moderate,finest,higher,highest,GREATEST(low,super_low,medium,above_moderate,finest,higher,highest)) as max_price_num from spliter");
    }

    public static Dataset<Row> flat(Dataset<Row> cumulative){
        String sql = "SELECT id as f_uid, max( IF ( act_type = 1, count, 0 ) ) AS type_1, max( IF ( act_type = 2, count, 0 )) AS type_2, max( IF ( act_type = 3, count, 0 )) AS type_3, max( IF ( act_type = 4, count, 0 )) AS type_4, max( IF ( act_type = 5, count, 0 )) AS type_5, max( IF ( act_type = 6, count, 0 )) AS type_6, max( IF ( act_type = 7, count, 0 )) AS type_7 FROM cumulative GROUP BY id";
        cumulative.createOrReplaceTempView("cumulative");
        return cumulative.sqlContext().sql(sql);
    }


    public static Dataset<Row> join(Dataset<Row> group,Dataset<Row> cumulative){

        return group.join(cumulative, group.col("id").equalTo(cumulative.col("f_uid"))).select("id","address","max_time","max_price","max_time_num","max_price_num","type_1","type_2","type_3","type_4","type_5","type_6","type_7");
    }
}
