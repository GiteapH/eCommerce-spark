package org.analysis.mllib.clv.utils;

import org.analysis.mllib.clv.Enum.rfmTagEnum;
import org.analysis.mllib.clv.UDF.getCLVGroupUDF;
import org.analysis.utils.commonAnalysisUtils;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.Objects;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 随机森林获取dateset的工具类
 * @date 2023/5/20 19:46
 */
public class commonUtils implements Serializable {
    public static Dataset<Row> getHistoryInfo(Dataset<Row> user,Dataset<Row> user_act,Dataset<Row> sku,Dataset<Row> rfmTag){
        user.createOrReplaceTempView("user");
        user_act.createOrReplaceTempView("user_act");
        sku.createOrReplaceTempView("sku");
        rfmTag.createOrReplaceTempView("rfm");
        user.sqlContext().udf().register("getAddress", (String addressF, String level) -> {
            try {
                return commonAnalysisUtils.addressResolution(addressF).getOrDefault(level, addressF);
            } catch (Exception e) {
                return addressF;
            }
        }, DataTypes.StringType);

        user.sqlContext().udf().register("rfm_tag_num", (String rfm) -> {
            return Objects.requireNonNull(rfmTagEnum.getTagEnum(rfm)).value;
        }, DataTypes.IntegerType);
//      user,sum(price) as price,IF(getAddress ( first(address), 'province' )='','无省份',getAddress ( first(address), 'province' )) AS province, IF(getAddress ( first(address), 'city' )='','无城市',getAddress ( first(address), 'city' )) AS city,CEILING(datediff ( current_date(), first(birthday))/365) as age,if(first(gender)='male',1,0) as gender,datediff(first(act_date),last(act_date))+1 as time_window,last(act_date),first(act_date)
        return user_act.sqlContext().sql("SELECT  user,sum(price) as price,IF(getAddress ( first(address), 'province' )='','无省份',getAddress ( first(address), 'province' )) AS province, IF(getAddress ( first(address), 'city' )='','无城市',getAddress ( first(address), 'city' )) AS city,CEILING(datediff ( current_date(), first(birthday))/365) as age,if(first(gender)='male',1,0) as gender,datediff(first(act_date),last(act_date))+1 as time_window,rfm_tag_num(if(first(rfm_tag) is null,'新客户',first(rfm_tag))) as rfm_tag FROM (select act_date,user_act.user,price,birthday,gender,rfm_tag,address from `user` LEFT JOIN `user_act` on `user`.id = user_act.`user`  LEFT JOIN sku ON user_act.sku = sku.sku_id LEFT JOIN rfm ON rfm.user = user_act.`user`  where act_type = 2 order by act_date desc) temp group by user order  by user");
    }

    public static Dataset<Row> checkEmpty(Dataset<Row> historyInfo){
        historyInfo.createOrReplaceTempView("history");
        return historyInfo.sqlContext().sql("select * from history where price ='' or features = null");
    }

    public static Dataset<Row> getPriceAssemble(Dataset<Row> row){
        // 将特征向量列合并为一个向量列
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"user","time_window","age","gender","provinceVec","cityVec","rfm_tag"})
                .setOutputCol("features");
        // 转换训练集和测试集的格式
        row = row.na().drop();
        return assembler.transform(row);
    }

    public static Dataset<Row> getLossAssemble(Dataset<Row> rows){
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"price","age","time_window","rfm_tag","provinceVec","cityVec"})
                .setOutputCol("features");
        // 转换训练集和测试集的格式
        rows = rows.na().drop();

        return assembler.transform(rows);
    }

    public static Dataset<Row> joinUserAndRule(Dataset<Row> rule_user,Dataset<Row> base_user,int time_window){
        base_user = stringFeatures.address2vec(base_user,"province,city,county,user_id, gender, age");
        rule_user.createOrReplaceTempView("rule_user");
        base_user.createOrReplaceTempView("base_user");
        rule_user.sqlContext().udf().register("rfm_tag_num", (String rfm) -> {
            return Objects.requireNonNull(rfmTagEnum.getTagEnum(rfm)).value;
        }, DataTypes.IntegerType);
        return base_user.sqlContext().sql("select base_user.user_id as user,province,city,county,provinceVec,cityVec,age,if(gender='male',1,0) as gender,"+time_window+" as time_window,rfm_tag_num(if(rfm_tag is null,'新客户',rfm_tag)) as rfm_tag from base_user left join rule_user on rule_user.user = base_user.user_id where base_user.user_id is not null order by base_user.user_id");
    }

    public static Dataset<Row> joinLossAndBase_users(Dataset<Row> info,int time_window){
        info.createOrReplaceTempView("info");
        String sql = "select info.user,price,age,gender,provinceVec,cityVec,"+time_window+" as time_window,rfm_tag from info";
        return info.sqlContext().sql(sql);
    }

    public static Dataset<Row> joinLossAndBase_users(Dataset<Row> loss_or_back,Dataset<Row> history){
        loss_or_back.createOrReplaceTempView("loss_or_back");
        history.createOrReplaceTempView("history");
        return history.sqlContext().sql("select history.user,provinceVec,cityVec,price,age,gender,time_window,if(loss_percent is null,rand() * 45,loss_percent) as loss_percent,if(loss_or_back.diff is null,1,loss_or_back.diff) as diff,rfm_tag from history LEFT join loss_or_back on loss_or_back.user = history.user where time_window != 1 order by history.user");
    }

//    整合价值预测，流逝预测，回归预测
    public static Dataset<Row> groupCLV(Dataset<Row> forecastPrice,Dataset<Row> forecastLoss,Dataset<Row> forecastBack){
        forecastPrice.createOrReplaceTempView("forecastPrice");
        forecastLoss.createOrReplaceTempView("forecastLoss");
        forecastBack.createOrReplaceTempView("forecastBack");
        SQLContext sqlContextPrice = forecastPrice.sqlContext();
        sqlContextPrice.udf().register("CLV_group",new getCLVGroupUDF(),DataTypes.StringType);
        double CLVAvg = sqlContextPrice.sql("select AVG(prediction) FROM forecastPrice").first().getDouble(0);
        return sqlContextPrice.sql("select user,province,city,county,age,if(gender=1,'male','female') as gender,time_window,rfm_tag,CLV_group(prediction,"+CLVAvg+") as user_value from forecastPrice inner join forecastLoss on forecastLoss.user=forecastPrice.user inner join forecastBack on forecastBack.user=forecastPrice.user");
    }
}
