package org.analysis.ruleTag.udf;

import org.analysis.Enum.SplitTimeDataEnum;
import org.apache.spark.sql.api.java.UDF7;

public class getTimeTagByValueUDF implements UDF7<Long,Long,Long,Long,Long,Long,Long,String> {
    @Override
    public String call(Long o1, Long o2, Long o3, Long o4, Long o5, Long o6, Long o7){
        if (o1.equals(o7)) {
            return SplitTimeDataEnum.DAWN.getValue();
        } else if (o2.equals(o7)) {
            return SplitTimeDataEnum.MORNGING.getValue();
        } else if (o3.equals(o7)) {
            return SplitTimeDataEnum.NOON.getValue();
        } else if (o4.equals(o7)) {
            return SplitTimeDataEnum.MIDDAY.getValue();
        } else if (o5.equals(o7)) {
            return SplitTimeDataEnum.AFTERNOON.getValue();
        } else if (o6.equals(o7)) {
            return SplitTimeDataEnum.EVENING.getValue();
        }
        return null;
    }
}
