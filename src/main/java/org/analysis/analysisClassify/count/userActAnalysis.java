package org.analysis.analysisClassify.count;

import org.analysis.utils.Logger;
import org.analysis.utils.commonAnalysisUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;


public class userActAnalysis {
    org.apache.log4j.Logger logger = Logger.getInstance();

//    统计关于商品，地址的用户行为次数
    public Dataset<Row> analysisActCount(Dataset<Row> user_act,Dataset<Row> users,boolean isGlobal) throws AnalysisException{
        try {
            if (isGlobal) {
                user_act.createGlobalTempView("user_act");
                users.createGlobalTempView("users");
            }else{
                user_act.createOrReplaceTempView("user_act");
                users.createOrReplaceTempView("users");
            }
            return user_act.sqlContext().sql("select address,act_type,sku, count(act_type) as count from user_act inner join users on user = id group by act_type,address,sku");
        }catch (AnalysisException e){
            logger.error(e.getMessage());
            throw e;
        }
    }


    public Dataset<Row> analysisStreamingData(Dataset<Row> user_act,Dataset<Row> base,Dataset<Row> sku){
        base.createOrReplaceTempView("base");
        user_act.createOrReplaceTempView("user_act");
        sku.createOrReplaceTempView("sku");
        return user_act.sqlContext().sql("select sku,price,act_type,user, act_date,act_time,gender,age,province,city,county from user_act inner join base on base.user_id = user inner join sku on sku.sku_id = user_act.sku");
    }


    public Dataset<Row> analysisUserActCount(Dataset<Row> user_act,Dataset<Row> user){
        user.createOrReplaceTempView("user");
        user_act.createOrReplaceTempView("user_act");
        return user_act.sqlContext().sql("select address,act_type,user, count(act_type) as count from user_act inner join user on user = id group by act_type,address,user");
    }

//  统计关于购买商品x的男女分布，年龄分布，地区分布
    public Dataset<Row> analysisCentralizedDistribution(Dataset<Row> user_act,Dataset<Row> users,boolean isGlobal) throws AnalysisException {
        try {
            if (isGlobal) {
                user_act.createGlobalTempView("user_act");
                users.createGlobalTempView("user");
            }else{
                user_act.createOrReplaceTempView("user_act");
                users.createOrReplaceTempView("user");
            }
            SQLContext context = user_act.sqlContext();
            context.udf().register("getAddress", (String addressF,String level)-> commonAnalysisUtils.addressResolution(addressF).get(level), DataTypes.StringType);
            return user_act.sqlContext().sql("SELECT sum( IF ( DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 < 5, 1, 0 )) AS less5, sum( IF ( DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 < 10 AND DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 >= 5, 1, 0 )) AS less10, sum( IF ( DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 < 15 AND DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 >= 10, 1, 0 )) AS less15, sum( IF ( DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 < 20 AND DATEDIFF( CURRENT_DATE, `user`.birthday )/ 365 >= 15, 1, 0 )) AS less20, sum( IF ( `user`.gender = 'female', 1, 0 )) AS female, sum( IF ( `user`.gender = 'male', 1, 0 )) AS male, count( `user`.address ) AS address_num, sku, `user`.address, getAddress ( `user`.address, 'province' ) AS province, getAddress ( `user`.address, 'city' ) AS city, getAddress ( `user`.address, 'county' ) AS county FROM user_act INNER JOIN `user` ON `user`.id = `user_act`.`user` WHERE act_type = 2 GROUP BY sku, `user`.address");
        }catch (AnalysisException e){
            logger.error(e.getMessage());
            throw e;
        }
    }


//    统计回购率
    public Dataset<Row> analysisRepurchase(Dataset<Row> user_act,Dataset<Row> users){
       user_act.createOrReplaceTempView("user_act");
        users.createOrReplaceTempView("user");
        return user_act.sqlContext().sql("SELECT IF(count( `user`.id ) > 1,1,0) AS repurchase,address,sku,`user`.id,count( `user`.id ) AS purchase_num,max(act_date) as act_date FROM user_act INNER JOIN `user` on `user`.id = `user_act`.`user` WHERE act_type = 2 GROUP BY sku, `user`.id, address");
    }


//    统计跳失率
    public Dataset<Row> analysisJumpLoss(Dataset<Row> user_act,Dataset<Row> users){
        user_act.createOrReplaceTempView("user_act");
        users.createOrReplaceTempView("user");
        return user_act.sqlContext().sql("SELECT sku,watch,count(watch) as watch_num from (select sku,user,IF(count(user)>1,1,0) as watch from user_act WHERE act_type = 1 GROUP BY sku,user) t GROUP BY sku,watch");
    }
//  用户每天的购买额，操作次数
    public Dataset<Row> analysisUserDaily(Dataset<Row> user_act,Dataset<Row> users){
        user_act.createOrReplaceTempView("user_act");
        users.createOrReplaceTempView("user");
        return user_act.sqlContext().sql("SELECT" +
                "t.`USER`," +
                "t.act_date," +
                "SUM(t.buy_line) as buy_line," +
                "MAX( CASE act_type WHEN 1 THEN num ELSE 0 END ) AS `view`," +
                "MAX( CASE act_type WHEN 2 THEN num ELSE 0 END ) AS buy," +
                "MAX( CASE act_type WHEN 3 THEN num ELSE 0 END ) AS fan," +
                "MAX( CASE act_type WHEN 4 THEN num ELSE 0 END ) AS `comment`," +
                "MAX( CASE act_type WHEN 5 THEN num ELSE 0 END ) AS cart," +
                "MAX( CASE act_type WHEN 6 THEN num ELSE 0 END ) AS consult," +
                "MAX( CASE act_type WHEN 7 THEN num ELSE 0 END ) AS conplain," +
                "AVG(diff)  as diff" +
                "FROM" +
                "(" +
                "SELECT USER" +
                "," +
                "act_date," +
                "act_type," +
                "count( act_type ) AS num," +
                "SUM(" +
                "IF" +
                "( act_type = 2, price, 0 )) AS buy_line " +
                "FROM" +
                "user_act" +
                "INNER JOIN sku ON sku.sku_id = user_act.sku " +
                "GROUP BY" +
                "USER," +
                "act_date," +
                "act_type " +
                "ORDER BY" +
                "`user` " +
                ") t" +
                "INNER JOIN (" +
                "SELECT" +
                "t1.`user`," +
                "t1.act_date," +
                "IF" +
                "(" +
                "DATEDIFF( t2.act_date, t1.act_date ) IS NULL," +
                "0," +
                "DATEDIFF( t2.act_date, t1.act_date )) AS diff " +
                "FROM" +
                "( SELECT USER, act_date, ROW_NUMBER() OVER ( PARTITION BY `user` ORDER BY act_date ) AS row_num FROM user_act GROUP BY act_date, USER ) t1" +
                "LEFT JOIN ( SELECT USER, act_date, ROW_NUMBER() OVER ( PARTITION BY `user` ORDER BY act_date ) AS row_num FROM user_act GROUP BY act_date, USER ) t2 ON t2.`user` = t1.`user` " +
                "AND t1.row_num = t2.row_num - 1 " +
                ") t4 ON t4.USER = t.`user` " +
                "AND t4.act_date = t.act_date " +
                "GROUP BY t.`USER`,t.act_date");
    }
}
