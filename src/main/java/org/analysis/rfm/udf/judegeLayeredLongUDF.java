package org.analysis.rfm.udf;

import org.analysis.rfm.config;
import org.apache.spark.sql.api.java.UDF2;

public class judegeLayeredLongUDF implements UDF2<Long,Integer,Integer> {
    @Override
    public Integer call(Long value, Integer type) throws Exception {
        int score = 1;
        score = config.judgeFrequency(value.intValue(),type);
        return score;
    }
}
