package org.analysis.ruleTag.udf;

import org.analysis.Enum.SplitPriceDataEnum;
import org.apache.spark.sql.api.java.UDF8;

public class getPriceTagByValueUDF implements UDF8<Long,Long,Long,Long,Long,Long,Long,Long,String> {

    @Override
    public String call(Long o1, Long o2, Long o3, Long o4, Long o5, Long o6, Long o7, Long o8) {
        if (o1.equals(o8)) {
            return SplitPriceDataEnum.Low.getValue();
        } else if (o2.equals(o8)) {
            return SplitPriceDataEnum.SuperLow.getValue();
        } else if (o3.equals(o8)) {
            return SplitPriceDataEnum.MEDIUM.getValue();
        } else if (o4.equals(o8)) {
            return SplitPriceDataEnum.ABOVEMODERATE.getValue();
        } else if (o5.equals(o8)) {
            return SplitPriceDataEnum.FINEST.getValue();
        } else if (o6.equals(o8)) {
            return SplitPriceDataEnum.HIGHER.getValue();
        } else if (o7.equals(o8)){
            return SplitPriceDataEnum.HIGHIEST.getValue();
        }
        return null;
    }
}
