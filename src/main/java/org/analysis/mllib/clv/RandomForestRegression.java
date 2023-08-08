package org.analysis.mllib.clv;

import org.analysis.mllib.clv.utils.commonUtils;
import org.analysis.mllib.clv.utils.stringFeatures;
import org.analysis.utils.Logger;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;


/**
 * @author 吕杨平
 * @version 1.0
 * @description: 随机森林预测未来支出模型
 * @date 2023/5/23 19:10
 */
public class RandomForestRegression {
    static org.apache.log4j.Logger logger = Logger.getInstance();
    public void run(Dataset<Row> user,Dataset<Row> user_act,Dataset<Row> sku,Dataset<Row> rfmTag,String model,int time){
        RandomForestRegression randomForestRegression = new RandomForestRegression();
        randomForestRegression.randomForestRegression(user, user_act, sku,rfmTag,model,time);
    }

    public Dataset<Row> randomForestRegression(Dataset<Row> user,Dataset<Row> user_act,Dataset<Row> sku,Dataset<Row> rfmTag,String modelPath,int time) {
        stringFeatures stringFeature = new stringFeatures();
        Dataset<Row> historyInfo = commonUtils.getHistoryInfo(user, user_act, sku,rfmTag);
        historyInfo = stringFeature.address2NumFeatrues(historyInfo);
        // 按照比例拆分数据集为训练集和测试集
        Dataset<Row>[] splits = historyInfo.randomSplit(new double[]{0.9, 0.1});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];
        // 将特征向量列合并为一个向量列
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"user","time_window","age","gender","provinceVec","cityVec","rfm_tag"})
                .setOutputCol("features");
        // 转换训练集和测试集的格式
        trainingData = trainingData.na().drop();
        Dataset<Row> train = assembler.transform(trainingData).select("price", "features");
        Dataset<Row> test = assembler.transform(testData).select("price", "features");
        // 创建随机森林回归器
        RandomForestRegressor rf = new RandomForestRegressor();
        // 设置模型参数
        rf.setMaxDepth(12); // 决策树的最大深度，避免过拟合
        rf.setNumTrees(75); // 森林中树的数量，越多模型越复杂，但也更容易过拟合
        rf.setFeatureSubsetStrategy("auto"); // 每个树使用的特征数量，"auto"表示自动选择
        rf.setLabelCol("price");
        rf.setFeaturesCol("features");
        // 训练模型
        train = train.na().drop();
        RandomForestRegressionModel model;
        if(modelPath==null) {
            model = rf.fit(train);
            try {
                model.write().overwrite().save("models/priceForecastModels/"+time);
            } catch (IOException e) {
                logger.error("模型保存失败");
            }
        }else{
            model = RandomForestRegressionModel.load("models/priceForecastModels/"+modelPath);
        }
        // 在测试集上进行预测
        Dataset<Row> predictions = model.transform(test);
        // 输出预测结果
        predictions.show();
        return predictions;
    }
}