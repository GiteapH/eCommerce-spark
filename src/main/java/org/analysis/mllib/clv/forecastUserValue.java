package org.analysis.mllib.clv;

import org.analysis.mllib.clv.utils.commonUtils;
import org.analysis.mllib.clv.utils.stringFeatures;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 预测用户未来价值
 * @date 2023/5/23 19:10
 */
public class forecastUserValue {
    public Dataset<Row> forecastPrice(Dataset<Row> rule_user, Dataset<Row> base_user, int model_time_window,int time_window) {
        RandomForestRegressionModel model = RandomForestRegressionModel.load("models/priceForecastModels/" + model_time_window);
            rule_user = stringFeatures.address2vec(rule_user, "user,price,province,city,age,gender,time_window,rfm_tag");
            Dataset<Row> priceFeatures = commonUtils.joinUserAndRule(rule_user, base_user, time_window);
            Dataset<Row> assemble = commonUtils.getPriceAssemble(priceFeatures);
            return model.transform(assemble);
    }


    public Dataset<Row> forecastLoss(Dataset<Row> historyinfo, int model_time_window, int time_window) {
        RandomForestRegressionModel model = RandomForestRegressionModel.load("models/lossForecastModels/" + model_time_window);
        Dataset<Row> lossFeatures = commonUtils.joinLossAndBase_users(historyinfo, time_window);
        Dataset<Row> lossAssemble = commonUtils.getLossAssemble(lossFeatures);
        return model.transform(lossAssemble);

    }

    public Dataset<Row> forecastBack(Dataset<Row> historyinfo, int model_time_window, int time_window){
        RandomForestRegressionModel model = RandomForestRegressionModel.load("models/backForecastModels/" + model_time_window);
        Dataset<Row> backFeatures = commonUtils.joinLossAndBase_users(historyinfo,time_window);
        Dataset<Row> backAssemble = commonUtils.getLossAssemble(backFeatures);
        return model.transform(backAssemble);
    }
}
