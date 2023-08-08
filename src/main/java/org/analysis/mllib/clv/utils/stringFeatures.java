package org.analysis.mllib.clv.utils;

import org.analysis.mllib.clv.UDF.CN2HashUnicodeUDF;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

/**
 * @author Administrator
 * @version 1.0
 * @description: TODO
 * @date 2023/5/20 19:59
 */
public class stringFeatures implements Serializable {
    public Dataset<Row> address2NumFeatrues(Dataset<Row> historyInfo){

        Dataset<Row> transformed = address2vec(historyInfo,"user,price,age,gender,time_window,rfm_tag");
        return transformed.select("user","time_window","price","age","gender","provinceVec","cityVec","rfm_tag");
    }

    public static Dataset<Row> address2vec(Dataset<Row> info,String commons) {
        info.createOrReplaceTempView("info");
        SQLContext sqlContext = info.sqlContext();
        sqlContext.udf().register("unicode", new CN2HashUnicodeUDF(), DataTypes.IntegerType);
        String sql = String.format("select unicode(province) as provinceVec,unicode(city) as cityVec,%s from info", commons);
        return sqlContext.sql(sql);
    }
}
