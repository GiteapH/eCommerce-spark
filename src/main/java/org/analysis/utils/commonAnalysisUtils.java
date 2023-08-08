package org.analysis.utils;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class commonAnalysisUtils implements Serializable {
    static org.apache.log4j.Logger logger = Logger.getInstance();
    public Dataset<Row> deleteEmptyRow(Dataset<Row> rows,int filedIdx) {
        return rows.filter((FilterFunction<Row>) row -> {
            return !row.getString(filedIdx).equals("")&&row.getString(filedIdx)!=null;
        });
    }

    public static long[] getCentralizedDistributionVals(Row row){
        long[] vals = new long[7];
        for(int i=0;i<vals.length;i++){
            vals[i] = row.getLong(i);
        }
        return vals;
    }


    public static Map<String,String> addressResolution(String address){
        address = address.replaceAll("辖[县|区]","");
        String regex="(?<province>[^省]+省|.+自治区|.+自治|上海市|北京市|天津市|重庆市)(?<city>[^市]+市|.+自治州|.+区)?(?<county>[^县]+县|.+市|.+区|.+镇|.+局|.*)";
        Matcher m= Pattern.compile(regex).matcher(address);
        String province=null,city=null,county=null,town=null,village=null;
        Map<String,String> row=null;
        while(m.find()){
            row=new LinkedHashMap<String,String>();
            province=m.group("province");
            row.put("province", province==null?"":province.trim());
            city=m.group("city");
            row.put("city", city==null?"":city.trim());
            county=m.group("county");
            row.put("county", county==null?"":county.trim());
        }
        return row;
    }

    public static void main(String[] args) {
        System.out.println(addressResolution(" 新疆维吾尔自治"));
    }
}
