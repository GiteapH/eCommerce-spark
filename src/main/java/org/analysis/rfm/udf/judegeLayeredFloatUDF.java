package org.analysis.rfm.udf;

import org.analysis.rfm.config;
import org.apache.spark.sql.api.java.UDF2;

public class judegeLayeredFloatUDF implements UDF2<Double,Integer,Integer> {
    @Override
    public Integer call(Double value, Integer type) throws Exception {
        int score = 1;
        score = config.judgeConsumptionCapacity(value.intValue(),type);
        return score;
    }
}
