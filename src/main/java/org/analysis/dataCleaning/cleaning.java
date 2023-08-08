package org.analysis.dataCleaning;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class cleaning implements Serializable {
    public static Dataset<Row> illegalBehavior(Dataset<Row> user_act,Dataset<Row> user){
        user_act.createOrReplaceTempView("user_act");
        user.createOrReplaceTempView("user");
        return user_act.sqlContext().sql("SELECT act_date,act_time, user,act_type, sku FROM user_act INNER JOIN `user` ON `user`.id = user_act.`user` WHERE birthday <= act_date");
    }


    public static Dataset<Row> cleanEmpty(Dataset<Row> rows,String ...keys){

        return rows.filter((FilterFunction<Row>) row -> {
            boolean f = false;
            for(String key:keys){
                if(row.getAs(key) !=null){
                    f = true;
                    break;
                }
            }
            return f;
        });
    }
}
