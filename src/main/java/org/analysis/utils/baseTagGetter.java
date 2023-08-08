package org.analysis.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

public class baseTagGetter implements Serializable {
    public static Dataset<Row> analysisBaseTag(Dataset<Row> user) {
        user.sqlContext().udf().register("getAddress", (String addressF, String level) -> {
            try {
                return commonAnalysisUtils.addressResolution(addressF).getOrDefault(level, addressF);
            } catch (Exception e) {
                return addressF;
            }
        }, DataTypes.StringType);
        user.createOrReplaceTempView("user");
        return user.sqlContext().sql("select id as user_id,gender, CEILING(datediff ( current_date(), birthday )/365) as age, IF(getAddress (address, 'province' )='','无省份',getAddress (address, 'province' )) AS province, IF(getAddress (address, 'city' )='','无城市',getAddress ( address, 'city' )) AS city    , getAddress ( `user`.address, 'county' ) AS county from user");
    }
}