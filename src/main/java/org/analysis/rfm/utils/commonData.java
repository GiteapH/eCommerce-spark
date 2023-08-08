package org.analysis.rfm.utils;

import java.io.Serializable;
import org.apache.spark.sql.*;

public class commonData implements Serializable {
    public static Dataset<Row> getMaxMinActDate(Dataset<Row> user_act){
        user_act.createOrReplaceTempView("user_act");
       return user_act.sqlContext().sql("select MAX(act_date),MIN(act_date) FROM user_act");
    }
}
