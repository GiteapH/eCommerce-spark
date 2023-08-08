package org.analysis;

import org.analysis.analysisClassify.calculate.quantileLoss;
import org.analysis.analysisClassify.calculate.skuPriceCalculate;
import org.analysis.analysisClassify.count.userActAnalysis;
import org.analysis.analysisClassify.dataCleaning.cleaning;
import org.analysis.db.DBAction;
import org.analysis.db.rfmDBAction;
import org.analysis.db.ruleDBAction;
import org.analysis.mllib.clv.RandomForestBack;
import org.analysis.mllib.clv.RandomForestLoss;
import org.analysis.mllib.clv.RandomForestRegression;
import org.analysis.mllib.clv.forecastUserValue;
import org.analysis.mllib.clv.utils.commonUtils;
import org.analysis.mllib.clv.utils.stringFeatures;
import org.analysis.rfm.rfmEnter;
import org.analysis.ruleTag.groupRule;
import org.analysis.ruleTag.ruleEnter;
import org.analysis.spark.sparkBuilder;
import org.analysis.utils.baseTagGetter;
import org.analysis.utils.commonAnalysisUtils;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.sql.SQLException;

public class Analysis implements Serializable {

    private final int[] FORECAST_TIME_WINDOWS = new int[]{5,7,14,30,60};
    
    userActAnalysis userActAnalysis = new userActAnalysis();
    DBAction dbAction = new DBAction();
    skuPriceCalculate skuPriceCalculate = new skuPriceCalculate();

    rfmEnter rfmEnter = new rfmEnter();
    RandomForestRegression randomForestRegression = new RandomForestRegression();
    RandomForestLoss randomForestLoss = new RandomForestLoss();
    RandomForestBack randomForestBack = new RandomForestBack();
    forecastUserValue forecastUserValue = new forecastUserValue();
    quantileLoss quantileLoss = new quantileLoss();
    ruleEnter ruleEnter = new ruleEnter();
    ruleDBAction ruleDBAction = new ruleDBAction();
    rfmDBAction rfmDBAction = new rfmDBAction();
    
    private DataSource dataSource;
    
    
    public static void main(String[] args) throws AnalysisException, SQLException {
        Analysis analysis = new Analysis(args[0], args[1], args[2]);
    }


    public void resetAct(String path){
        dataSource.resetAct(path);
    }


    public Dataset<Row> analysisStreaming(Dataset<Row> base){
       return userActAnalysis.analysisStreamingData(dataSource.user_act,base,dataSource.sku);
    }

    /**
    *@author 吕杨平
    *@Description  数据清洗
    *@Date 18:48 2023/7/25
    *@Param [dataSource]
    *@Return void
    */
    public void clear(){
        System.err.println("清空无用行");
        dataSource.user_act = cleaning.cleanEmpty(dataSource.user_act,"act_type");
//        System.err.println("清空"+(before- user_act.count())+"无用行");
        //        格式化日期
        System.err.println("格式化日期");
        dataSource.user_act = cleaning.formatDate(dataSource.user_act, "act_time, user,act_type, sku", "act_date");
        dataSource.user_act = cleaning.formatTime(dataSource.user_act, "act_date, user,act_type, sku", "act_time");
//        不符合逻辑的用户行为
        System.err.println("清空不符合逻辑的用户行为");
        dataSource.user_act = cleaning.illegalBehavior(dataSource.user_act,dataSource.user);
    }

    /**
    *@author 吕杨平
    *@Description 复购率分析
    *@Date 18:49 2023/7/25
    *@Param [dataSource]
    *@Return void
    */
    public void anaysisRepurchase() throws SQLException {
        Dataset<Row> repurchase = userActAnalysis.analysisRepurchase(dataSource.user_act, dataSource.user);
        dbAction.insertRepurchase(repurchase);
    }

    /**
    *@author 吕杨平
    *@Description 跳失率分析
    *@Date 18:49 2023/7/25
    *@Param [dataSource]
    *@Return void
    */
    public void analysisJumploss(){
        Dataset<Row> jumpLoss = userActAnalysis.analysisJumpLoss(dataSource.user_act, dataSource.user);
        dbAction.insertJumpLoss(jumpLoss);
    }

    /**
    *@author 吕杨平
    *@Description 规则统计
    *@Date 18:49 2023/7/25
    *@Param [dataSource]
    *@Return void
    */
    public void anaysisRule() throws SQLException {
        //        计算规则标签
        for(int i = 2;i<=4;i++){
            Dataset<Row> splitter = ruleEnter.analysisSplitter(dataSource.user, dataSource.user_act, dataSource.sku,i);
            splitter.show();
            ruleDBAction.insertSplitter(splitter);
            Dataset<Row> cumulative = ruleEnter.analysisCumulative(dataSource.user, dataSource.user_act, i);
            Dataset<Row> group = groupRule.group(splitter);
            cumulative = groupRule.flat(cumulative);
            Dataset<Row> join = groupRule.join(group, cumulative);
            System.err.println("------------------------------------------");
            ruleDBAction.insertRuleTag(join,i);
        }

    }

    /**
    *@author 吕杨平
    *@Description 行为计数
    *@Date 18:49 2023/7/25
    *@Param [dataSource]
    *@Return void
    */
    public void anaysisCount() throws AnalysisException, SQLException {
        Dataset<Row> count = userActAnalysis.analysisActCount(dataSource.user_act,dataSource.user, false);
        Dataset<Row> userActCount = userActAnalysis.analysisUserActCount(dataSource.user_act, dataSource.user);
        dbAction.insertCountFull(count);
        dbAction.insertActCount(userActCount);
    }

    /**
    *@author 吕杨平
    *@Description 商品交易分布信息
    *@Date 18:49 2023/7/25
    *@Param [dataSource]
    *@Return void
    */
    public void anaysisCentralizedDistribution() throws AnalysisException {
        Dataset<Row> centralizedDistributionDataset = userActAnalysis.analysisCentralizedDistribution(dataSource.user_act,dataSource.user,false);
        dbAction.insertCentralizedDistribution(centralizedDistributionDataset);
    }

    /**
    *@author 吕杨平
    *@Description CLV分析
    *@Date 18:50 2023/7/25
    *@Param [time, dataSource, model, rfmTag, baseTag]
    *@Return void
    */
    public void anaysisCLV(int time,String model,Dataset<Row> rfmTag,Dataset<Row> baseTag) throws SQLException {
            randomForestRegression.run(dataSource.user, dataSource.user_act, dataSource.sku, rfmTag, model, time);
            randomForestLoss.run(dataSource.user,dataSource.user_act,dataSource.sku,rfmTag,quantileLoss.lossUser,model,time);
            randomForestBack.run(dataSource.user,dataSource.user_act,dataSource.sku,rfmTag,quantileLoss.backUser,model,time);
            Dataset<Row> historyInfo = commonUtils.getHistoryInfo(dataSource.user, dataSource.user_act, dataSource.sku,rfmTag);
            historyInfo = stringFeatures.address2vec(historyInfo,"dataSource.user,price,province,city,age,gender,time_window,rfm_tag");
            for(int time_window:FORECAST_TIME_WINDOWS){
                Dataset<Row> forecastLoss = forecastUserValue.forecastLoss(historyInfo, time,time_window);
                Dataset<Row> forecastPrice = forecastUserValue.forecastPrice(rfmTag, baseTag, time, time_window);
                Dataset<Row> forecastBack = forecastUserValue.forecastBack(historyInfo, time, time_window);
                Dataset<Row> rowDataset = commonUtils.groupCLV(forecastPrice, forecastLoss, forecastBack);
            }
//        }
        
    }

    /**
    *@author 吕杨平
    *@Description RFM分析
    *@Date 18:50 2023/7/25
    *@Param [time, dataSource]
    *@Return void
    */
    public void anaysisRFM(int time) throws SQLException {
//          rfm规则分析
//        for(int i=1;i<3;i++) {
            Dataset<Row> rfm = rfmEnter.rfm(dataSource.user, dataSource.user_act, dataSource.sku, time);
            Dataset<Row> rfmTag = rfmEnter.analysisRfmTag(rfm);
            rfmDBAction.insertrfm(rfmTag, time);
//        }
    }

    /**
    *@author 吕杨平
    *@Description 计算流失率  getter之后才可以获取流失用户
    *@Date 12:16 2023/7/25
    *@Param [dataSource]
    *@Return void
    */
    public void anaysisBackLossUser() throws SQLException {
        quantileLoss.getter(dataSource.user_act,false);
        ruleDBAction.insertTransactionInterval(quantileLoss.transactionInterval);
        ruleDBAction.insertUserIdentity(quantileLoss.lossUser,quantileLoss.backUser);

    }

    public Dataset<Row> analysisBase(){
//        格式化user，并进行基础标签分析
        Dataset<Row> baseTag = baseTagGetter.analysisBaseTag(dataSource.user);
        return baseTag;
//        dbAction.insertBaseTag(baseTag);
    }
    /**
    *@author 吕杨平
    *@Description 分析商品价格
    *@Date 14:04 2023/7/27
    *@Param []
    *@Return void
    */
    public void anaysisSkuPrice() throws SQLException, AnalysisException {
        Dataset<Row> skuPrice = skuPriceCalculate.calculatePrice(dataSource.sku, dataSource.user_act,dataSource.user, false);
        dbAction.insertSkuPrice(skuPrice);
    }

     public Analysis(String userPath,String skuPath,String actPath) throws AnalysisException, SQLException {
        this.dataSource = new DataSource(userPath, skuPath, actPath);
//        clear();
    }
}