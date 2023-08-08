package org.analysis.analysisClassify.calculate;

import org.analysis.utils.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;


public class skuPriceCalculate implements Serializable {
    org.apache.log4j.Logger logger = Logger.getInstance();

//    关于产品的销售额分析
    public Dataset<Row> calculatePrice(Dataset<Row> sku,Dataset<Row> act,Dataset<Row> user, boolean isGlobal) throws AnalysisException {
        try {
            if (isGlobal) {
                act.createGlobalTempView("user_act");
                sku.createGlobalTempView("sku");
                user.createGlobalTempView("user");
            }else{
                act.createOrReplaceTempView("user_act");
                sku.createOrReplaceTempView("sku");
                user.createOrReplaceTempView("user");
            }
//            获取产品下单数量和销售额
            String sql = "SELECT sku,price * count( sku ) AS sum_price,count( sku ) AS num,address FROM user_act INNER JOIN sku ON sku = sku_id  INNER JOIN user ON user.id = user_act.`user` WHERE act_type = 2 GROUP BY sku,address,price ORDER BY num DESC";
            return sku.sqlContext().sql(sql);
        }catch (AnalysisException e){
            logger.error(e.getMessage());
            throw e;
        }
    }
}
