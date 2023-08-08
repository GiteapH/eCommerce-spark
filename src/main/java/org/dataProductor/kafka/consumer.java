package org.dataProductor.kafka;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

import org.analysis.Analysis;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.analysis.rfm.db.commenDBAction;
import org.dataProductor.utils.csv;
import org.dataProductor.utils.mallActProductorUtils;

public class consumer {

    public static void main(String[] args) throws SQLException, AnalysisException {
        List<String[]> rows = new ArrayList<>();
        commenDBAction commenDBAction = new commenDBAction();
        Calendar startDate = Calendar.getInstance();
        startDate.setTime(new Date());
        Analysis analysis = new Analysis("E:/数据集/user/user.csv","E:/数据集/user/sku.csv","temp/csv/temp_user_act.csv");
        SimpleDateFormat stf_com = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
        SimpleDateFormat stf_date = new SimpleDateFormat("yy/MM/dd");
        SimpleDateFormat stf_time = new SimpleDateFormat("HH:mm:ss");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test_20210816_2"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                List<String[]> update = new ArrayList<>();
                for(String row:record.value().split("\n")){
                    rows.add(row.split(","));
                    update.add(row.split(","));
                }
                //            插入每次更新数据
                csv.create("temp/csv/temp_update_user_act.csv", update, "\"act_date\"", "\"act_time\"", "\"user\"", "\"act_type\"", "\"sku\"");
                analysis.resetAct("temp/csv/temp_update_user_act.csv");
                Dataset<Row> base = analysis.analysisBase();
                Dataset<Row> streaming_update = analysis.analysisStreaming(base);
                try {
                    streaming_update.show();
                    commenDBAction.insertUpdateStream(streaming_update);
                }catch (SQLException e){
                    e.printStackTrace();
                }

            }
            if(rows.size()>100000){
//                插入大批量更新数据
                csv.create("temp/csv/temp_user_act.csv", rows, "\"act_date\"", "\"act_time\"", "\"user\"", "\"act_type\"", "\"sku\"");
                analysis.resetAct("temp/csv/temp_user_act.csv");
                Dataset<Row> base_s = analysis.analysisBase();
                Dataset<Row> streaming = analysis.analysisStreaming(base_s);
                try {
                    String[] dateDiff = mallActProductorUtils.calcurlateDateDiff(startDate);
                    commenDBAction.insertStream(streaming);
                    Date update_time = startDate.getTime();
                    commenDBAction.updateStreamingInfo(dateDiff[0],stf_com.format(mallActProductorUtils.calcurlateDate(startDate,Integer.parseInt(dateDiff[1]))) ,String.valueOf(rows.size()),String.valueOf(rows.size()),stf_date.format(update_time),stf_time.format(update_time));
                }catch (SQLException e){
                    e.printStackTrace();
                }
                rows.clear();
            }else{
                commenDBAction.updateStreamingInfo(null,null,null,String.valueOf(rows.size()),null,null);
            }
        }
    }
}