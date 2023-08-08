package org.analysis.analysisClassify.count;

import org.analysis.utils.commonAnalysisUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

/**
*@author 吕杨平
*@Description 用户基础标签获取
*@Date 11:51 2023/4/30
*/
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

        return user.sqlContext().sql("select id as user_id,gender, CEILING(datediff ( current_date(), birthday )/365) as age, getAddress ( `user`.address, 'province' ) AS province, getAddress ( `user`.address, 'city' ) AS city, getAddress ( `user`.address, 'county' ) AS county from user");

    }
}