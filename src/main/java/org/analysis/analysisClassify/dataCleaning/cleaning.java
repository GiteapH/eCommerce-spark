package org.analysis.analysisClassify.dataCleaning;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class cleaning implements Serializable {
    public static Dataset<Row> illegalBehavior(Dataset<Row> user_act,Dataset<Row> user){
        user_act.createOrReplaceTempView("user_act");
        user.createOrReplaceTempView("user");
        return user_act.sqlContext().sql("SELECT act_date,act_time, user,act_type, sku FROM user_act INNER JOIN `user` ON `" +
                "user`.id = user_act.`user` WHERE birthday <= act_date");
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

    public static Dataset<Row> formatDate(Dataset<Row> table,String commons,String date){
        table.createOrReplaceTempView("table");
        String sql = String.format("date_format(REGEXP_REPLACE(%s,'/','-'),'yyyy-MM-dd') as %s,%s",date,date,commons);
        return table.sqlContext().sql("select "+sql+" from table");
    }

    public static  Dataset<Row> formatTime(Dataset<Row> table,String commons,String time){
        table.createOrReplaceTempView("table");
        String sql = String.format("date_format(%s,'HH:mm:ss') as %s,%s",time,time,commons);
        return table.sqlContext().sql("select "+sql+" from table");
    }
}
