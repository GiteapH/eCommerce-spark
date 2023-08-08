package org.analysis.utils;

import org.apache.log4j.BasicConfigurator;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;


public class userActAnalysis {
    org.apache.log4j.Logger logger = Logger.getInstance();
    public Dataset<Row> analysisActCount(Dataset<Row> user_act,Dataset<Row> users,boolean isGlobal) throws AnalysisException{
        try {
            if (isGlobal) {
                user_act.createGlobalTempView("user_act");
                users.createGlobalTempView("users");
            }else{
                user_act.createOrReplaceTempView("user_act");
                users.createOrReplaceTempView("users");
            }
            return user_act.sqlContext().sql("select address,act_type,sku, count(act_type) as count from user_act inner join users on sku = id group by act_type,address,sku");
        }catch (AnalysisException e){
            logger.error(e.getMessage());
            throw e;
        }
    }


    public Dataset<Row> analysisCentralizedDistribution(Dataset<Row> user_act,Dataset<Row> users,boolean isGlobal) throws AnalysisException {
        try {
            if (isGlobal) {
                user_act.createGlobalTempView("user_act");
                users.createGlobalTempView("user");
            }else{
                user_act.createOrReplaceTempView("user_act");
                users.createOrReplaceTempView("user");
            }
            user_act.show();
            SQLContext context = user_act.sqlContext();
            context.udf().register("getAddress", (String addressF,String level)-> {
                return commonAnalysisUtils.addressResolution(addressF).get(level);
            }, DataTypes.StringType);
            Dataset<Row> baseCentralizedDistribution = user_act.sqlContext().sql("SELECT sum( IF ( DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 < 5, 1, 0 )) AS less5, sum( IF ( DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 < 10 AND DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 >= 5, 1, 0 )) AS less10, sum( IF ( DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 < 15 AND DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 >= 10, 1, 0 )) AS less15, sum( IF ( DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 < 20 AND DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 >= 15, 1, 0 )) AS less20, sum( IF ( `user`.gender = 'female', 1, 0 )) AS female, sum( IF ( `user`.gender = 'male', 1, 0 )) AS male, count( `user`.address ) AS address_num, sku, `user`.address, getAddress ( `user`.address, 'province' ) AS province, getAddress ( `user`.address, 'city' ) AS city, getAddress ( `user`.address, 'county' ) AS county FROM user_act INNER JOIN `user` ON `user`.id = `user_act`.`user` WHERE act_type = 2 GROUP BY sku, `user`.address");
            baseCentralizedDistribution.show();
            return baseCentralizedDistribution;
        }catch (AnalysisException e){
            logger.error(e.getMessage());
            throw e;
        }
    }


    public Dataset<Row> analysisRepurchase(Dataset<Row> user_act,Dataset<Row> users){
       user_act.createOrReplaceTempView("user_act");
        users.createOrReplaceTempView("user");
        return user_act.sqlContext().sql("SELECT IF(count( `user`.id ) > 1,1,0) AS repurchase,address,sku,`user`.id,count( `user`.id ) AS purchase_num FROM user_act INNER JOIN `user` on `user`.id = `user_act`.`user` WHERE act_type = 2 GROUP BY sku, `user`.id, address");
    }

    public Dataset<Row> analysisJumpLoss(Dataset<Row> user_act,Dataset<Row> users){
        user_act.createOrReplaceTempView("user_act");
        users.createOrReplaceTempView("user");
        return user_act.sqlContext().sql("SELECT user_act.`user`, act_type, sku,count(act_type) FROM user_act GROUP BY user_act.`user`, act_type, sku ORDER BY sku, act_type");
    }
}
