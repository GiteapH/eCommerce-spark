package org.analysis.mllib.clv;

import org.analysis.mllib.clv.utils.commonUtils;
import org.analysis.mllib.clv.utils.stringFeatures;
import org.analysis.utils.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;

/**
 * @author Administrator
 * @version 1.0
 * @description: TODO
 * @date 2023/5/25 15:05
 */
public class RandomForestBack {
    static org.apache.log4j.Logger logger = Logger.getInstance();
    public void run(Dataset<Row> user, Dataset<Row> user_act, Dataset<Row> sku, Dataset<Row> rfmTag,Dataset<Row> back, String model, int time){
        RandomForestBack randomForestBack = new RandomForestBack();
        randomForestBack.randomForestRegression(user, user_act, sku,rfmTag,back,model,time);
    }

    public Dataset<Row> randomForestRegression(Dataset<Row> user,Dataset<Row> user_act,Dataset<Row> sku,Dataset<Row> rfmTag,Dataset<Row> back,String modelPath,int time) {
        Dataset<Row> historyInfo = commonUtils.getHistoryInfo(user, user_act, sku,rfmTag);
        System.err.println(1);
        historyInfo = stringFeatures.address2vec(historyInfo,"user,price,province,city,age,gender,time_window,rfm_tag");
        Dataset<Row> Info = commonUtils.joinLossAndBase_users(back,historyInfo);
        // 按照比例拆分数据集为训练集和测试集
        Dataset<Row>[] splits = Info.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];
        // 将特征向量列合并为一个向量列
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"price","time_window","rfm_tag","age","provinceVec","cityVec"})
                .setOutputCol("features");
        // 转换训练集和测试集的格式
        trainingData = trainingData.na().drop();
        Dataset<Row> train = assembler.transform(trainingData).select("loss_percent", "features");
        Dataset<Row> test = assembler.transform(testData).select("loss_percent", "features");
        // 创建随机森林回归器
        RandomForestRegressor  ln = new RandomForestRegressor();
        // 设置模型参数
        ln.setMaxDepth(13); // 决策树的最大深度，避免过拟合
        ln.setNumTrees(75); // 森林中树的数量，越多模型越复杂，但也更容易过拟合
        ln.setFeatureSubsetStrategy("auto"); // 每个树使用的特征数量，"auto"表示自动选择
        ln.setLabelCol("loss_percent");
        // 训练模型
        train = train.na().drop();
        RandomForestRegressionModel model;
        if(modelPath==null) {
            model = ln.fit(train);
            try {
                model.write().overwrite().save("models/backForecastModels/"+time);
            } catch (IOException e) {
                logger.error("模型保存失败");
            }
        }else{
            model = RandomForestRegressionModel.load("models/backForecastModels/"+modelPath);
        }
        // 在测试集上进行预测
        Dataset<Row> predictions = model.transform(test);
        // 输出预测结果
        predictions.show();
        return predictions;
    }
}
