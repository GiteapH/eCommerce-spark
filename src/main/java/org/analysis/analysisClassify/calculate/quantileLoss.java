package org.analysis.analysisClassify.calculate;

import org.analysis.db.ruleDBAction;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.sql.SQLException;

/**
*@author 吕杨平
*@Description 使用分位数法计算用户流失率
*@Date 11:48 2023/4/30
*/
public class quantileLoss implements Serializable {

   public Dataset<Row> lossUser,backUser,transactionInterval;
   public int quantityNum;
   public double[] back_loss_percents = new double[2];
//    入口方法
    public void getter(Dataset<Row> user_act,boolean calcurlate) throws SQLException {
        transactionInterval = calculateTransactionInterval(user_act);

        quantityNum = getQuantityNum(transactionInterval, 95f);
//
        lossUser = getLossUser(transactionInterval, quantityNum);

        backUser = getBackUser(transactionInterval,quantityNum);

        if(calcurlate){
            long lossCount = lossUser.count();
            long backCount = backUser.count();
            long count = transactionInterval.count();
            this.back_loss_percents = new double[]{backCount/count,lossCount/count};
            System.err.println("回归用户:"+backCount);
        }
    }


//    计算每个用户交易时间间隔
    public Dataset<Row> calculateTransactionInterval(Dataset<Row> user_act) {
        user_act.createOrReplaceTempView("user_act");
        SQLContext sqlContext = user_act.sqlContext();
        String sql = "SELECT t1.act_date,t1.act_time, t1.`user`, DATEDIFF(t2.act_date, t1.act_date) as diff FROM(SELECT *," +
                " ROW_NUMBER() OVER ( PARTITION BY `user` ORDER BY act_date,act_time) AS row_num FROM user_act) t1 LEFT JOIN " +
                "(SELECT *, ROW_NUMBER() OVER ( PARTITION BY `user` ORDER BY act_date,act_time) AS row_num FROM user_act) t2 ON t2.`user` = t1.`user` " +
                "AND t1.row_num = t2.row_num-1 order by diff desc, t1.user,t1.act_date,t1.act_time";
//
//        String sql ="SELECT *,date_format(REGEXP_REPLACE(act_date,'/','-'),'yyyy-MM-dd') FROM user_act order by user,date_format(REGEXP_REPLACE(act_date,'/','-'),'yyyy-MM-dd'),act_time";
        Dataset<Row> transactionInterval = sqlContext.sql(sql);
        return transactionInterval.filter((FilterFunction<Row>) row -> row.getAs("diff")!=null);
    }


    /**
    *@author 吕杨平
    *@Description 获取交易间隔分位数
    *@Date 14:05 2023/5/1
    *@Param [累计间隔所占百分比，默认为90%]
    *@Return 间隔数
    */
    public int getQuantityNum(Dataset<Row> transactionInterval,float targetProportion){
        float tempProportion = 0f;
        transactionInterval.createOrReplaceTempView("transaction_interval");
        SQLContext sqlContext = transactionInterval.sqlContext();
        long allQuantities = sqlContext.sql("SELECT count(*) as allQuantities from transaction_interval").first().getLong(0);
        String sql = String.format("select diff,count(diff)/%d*100 as proportion from transaction_interval GROUP BY diff ORDER BY diff",allQuantities);
        Dataset<Row> searchQuantityNum = sqlContext.sql(sql);
//        提取前1000个，大几率满足大于targetProportion(全选会爆栈)
        Row[] collectRows = (Row[]) searchQuantityNum.take(1000);
        for(Row row:collectRows){
            if(tempProportion>=targetProportion){
                return row.getInt(0);
            }else{
                tempProportion+=row.getDouble(1);
            }
        }
        return 5;
    }


    /**
    *@author 吕杨平
    *@Description 找出流失用户
    *@Date 14:59 2023/5/1
    *@Return 流失用户数
    */
    public Dataset<Row> getLossUser(Dataset<Row> transactionInterval,int quantityNum){
        transactionInterval.createOrReplaceTempView("transaction_interval");
        String loss = String.format("SELECT user,(rand() *19.1 + 70) as loss_percent, act_date, act_time, diff FROM ( SELECT user, first(act_date) as act_date, first(act_time) as act_time, first(diff) as diff FROM( SELECT * FROM transaction_interval ORDER BY user, act_date DESC, act_time DESC LIMIT 50000000) t GROUP BY user) t2 where diff >%d",quantityNum,quantityNum);
//        流失用户
        return transactionInterval.sqlContext().sql(loss);
    }

    public Dataset<Row> getBackUser(Dataset<Row> transactionInterval, int quantityNum){
        System.err.println(quantityNum);
        transactionInterval.createOrReplaceTempView("transaction_interval");
        SQLContext sqlContext = transactionInterval.sqlContext();
        //        回归用户(存在间隔>quantityNum,且最新间隔<quantityNum)
        String back = String.format("SELECT user,(rand() *19.1 + 80) as loss_percent,act_date, act_time, diff FROM ( SELECT user,first(act_date) as act_date, first(act_time) as act_time, first(diff) as diff FROM( SELECT * FROM transaction_interval WHERE user IN ( SELECT user FROM transaction_interval WHERE diff > %d group by user) ORDER BY user, act_date DESC, act_time DESC LIMIT 50000000 ) t GROUP BY user ) t2 WHERE diff <=%d order by user",quantityNum,quantityNum,quantityNum);
        Dataset<Row> backUser = sqlContext.sql(back);
        return backUser;
    }
}
