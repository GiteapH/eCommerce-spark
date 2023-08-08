package org.analysis.spark;

import org.analysis.db.rfmDBAction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
public class sparkBuilder {
    private final DataFrameReader reader;
    private static sparkBuilder sparkBuilder;

    private sparkBuilder(){
        try {
            SparkConf sparkConf = new SparkConf();
            sparkConf.setMaster("local[*]").setAppName("goodsAnalysis").set("spark.executor.memory","12g").set("spark.driver.memory","12g").set("spark.testing.memory","2147480000");
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
            reader = new SQLContext(sparkContext).read().option("encoding", "gbk").format("com.databricks.spark.csv");
        }catch(Exception e){
            e.printStackTrace();
            throw e;
        }
    }

    public static sparkBuilder getSparkBuilder(){
        if(sparkBuilder==null)
            sparkBuilder = new sparkBuilder();
        return sparkBuilder;
    }

    public DataFrameReader setEncoding(String code){
        return reader.option("encoding",code);
    }

    public DataFrameReader setHeader(String header){
        return reader.option("header",header);
    }

    public DataFrameReader setInferSchema(String InferSchema){
        return reader.option("inferSchema",InferSchema);
    }

    public DataFrameReader setMultiLine(String multiLine){
        return reader.option("multiLine",multiLine);
    }

    public Dataset<Row> getDatasetByPath(String path){
        return reader.option("encoding", "gbk")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", "true")
                .load(path);
    }
}
