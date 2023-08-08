package org.analysis.rfm.udf;

import org.apache.spark.sql.api.java.UDF3;

public class toUserTypeUDF implements UDF3<String,String,String,String> {
    @Override
    public String call(String r, String f,String m) throws Exception {
        String rfmStr = r+f+m;
        switch (rfmStr) {
            case "高高高":
                return "重要价值客户";
            case "低高高":
                return "重要唤回客户";
            case "高低高":
                return "重要深耕客户";
            case "低低高":
                return "重要挽留客户";
            case "高高低":
                return "潜力客户";
            case "高低低":
                return "新客户";
            case "低高低":
                return "一般维持客户";
            case "低低低":
                return "流失客户";
        }
        return "未知类型";
    }
}
