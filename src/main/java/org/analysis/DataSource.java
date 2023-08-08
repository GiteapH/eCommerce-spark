package org.analysis;

import org.analysis.spark.sparkBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Administrator
 * @version 1.0
 * @description: 电商交易数据源
 * @date 2023/7/25 11:46
 */
public class DataSource {
    public Dataset<Row> user;

    public Dataset<Row> user_act;

    public Dataset<Row> sku;

    private org.analysis.spark.sparkBuilder sparkBuilder = org.analysis.spark.sparkBuilder.getSparkBuilder();
    public DataSource(String userPath,String skuPath,String actPath) {
        System.out.println("正在从hdfs中读取user.csv");
//        获取user CSV 用户数据
        user = sparkBuilder.getDatasetByPath(userPath);
        System.out.println("user 读取完成");
        System.out.println("正在从hdfs中读取sku.csv");
//        获取sku CSV 商品数据
        sku = sparkBuilder.getDatasetByPath(skuPath);
        System.out.println("sku 读取完成");
//        获取user_act CSV 用户动作
//        System.out.println("正在从hdfs中读取user_act.csv");
//        user_act = sparkBuilder.getDatasetByPath(actPath);
//        System.out.println("act 读取完成");
    }

    public void resetAct(String actPath){
        user_act = sparkBuilder.getDatasetByPath(actPath);
        System.out.println("act 更新读取完成");
    }
}
