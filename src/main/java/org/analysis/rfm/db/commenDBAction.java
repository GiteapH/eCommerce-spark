package org.analysis.rfm.db;

import org.analysis.Enum.ruleTableEnum;
import org.analysis.utils.ProgressUtil;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import static org.analysis.db.DBAction.conn;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 插入新记录
 * @date 2023/7/26 12:01
 */
public class commenDBAction implements Serializable {
    /**
     *@author 吕杨平
     *@Description 插入新数据
     *@Date 12:00 2023/7/26
     *@Param [stream_data, time]
     *@Return void
     */
    public void insertStream(Dataset<Row> stream_data) throws SQLException {
        ProgressUtil pu = new ProgressUtil("insert streaming(插入新数据中)", stream_data.count()/1000);
//        首先清空表
        Statement statement = conn.createStatement();
        statement.execute("truncate table streaming_user_act");
        conn.commit();
        stream_data.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement ruleStatement = conn.prepareStatement("insert into streaming_user_act values (?,?,?,?,?,?,?,?,?,?,?)");
                int i = 1;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    try {
                        ruleStatement.setString(1, row.getAs("act_date"));
                        ruleStatement.setString(2, row.getAs("act_time"));
                        ruleStatement.setInt(3, row.getAs("user"));
                        ruleStatement.setInt(4, row.getAs("act_type"));
                        ruleStatement.setInt(5, row.getAs("sku"));
                        ruleStatement.setDouble(6, row.getAs("price"));
                        ruleStatement.setString(7, row.getAs("province"));
                        ruleStatement.setString(8, row.getAs("city"));
                        ruleStatement.setString(9, row.getAs("county"));
                        ruleStatement.setString(10, row.getAs("gender"));
                        ruleStatement.setLong(11, row.getAs("age"));
                        ruleStatement.addBatch();
                        if (i % 1000 == 0) {
                            ruleStatement.executeBatch();
                            conn.commit();
                            ruleStatement.clearBatch();
                            pu.finishOne("完成量" + i);
                        }

                        i++;
                    } catch (Exception normal) {
                        normal.printStackTrace();
                        pu.failureOne("失败量" + row);
                    }
                }
                ruleStatement.executeBatch();
                conn.commit();
            }
        });
    }

    /**
     *@author 吕杨平
     *@Description 插入新数据
     *@Date 12:00 2023/7/26
     *@Param [stream_data, time]
     *@Return void
     */
    public void insertUpdateStream(Dataset<Row> stream_data) throws SQLException {
        System.out.println(6456456);
        ProgressUtil pu = new ProgressUtil("insert streaming(插入新数据中)",1000);
//        首先清空表
        Statement statement = conn.createStatement();
        statement.execute("truncate table streaming_update_user_act");
        conn.commit();
        System.out.println("已清空");
        stream_data.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement ruleStatement = conn.prepareStatement("insert into streaming_update_user_act values (?,?,?,?,?,?,?,?,?,?,?)");
                int i = 1;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    try {
                        ruleStatement.setString(1, row.getAs("act_date"));
                        ruleStatement.setString(2, row.getAs("act_time"));
                        ruleStatement.setInt(3, row.getAs("user"));
                        ruleStatement.setInt(4, row.getAs("act_type"));
                        ruleStatement.setInt(5, row.getAs("sku"));
                        ruleStatement.setDouble(6, row.getAs("price"));
                        ruleStatement.setString(7, row.getAs("province"));
                        ruleStatement.setString(8, row.getAs("city"));
                        ruleStatement.setString(9, row.getAs("county"));
                        ruleStatement.setString(10, row.getAs("gender"));
                        ruleStatement.setLong(11, row.getAs("age"));
                        ruleStatement.addBatch();
                        if (i % 100 == 0) {
                            ruleStatement.executeBatch();
                            conn.commit();
                            ruleStatement.clearBatch();
                            pu.finishOne("完成量" + i);
                        }

                        i++;
                    } catch (Exception normal) {
                        normal.printStackTrace();
                        pu.failureOne("失败量" + row);
                    }
                }
                ruleStatement.executeBatch();
                conn.commit();
            }
        });
    }
    
    /**
    *@author 吕杨平
    *@Description 轮询结束更新数据
    *@Date 20:09 2023/7/26
    *@Param [complete_time, next_poll, records, temp_cumcumlative, update_date, update_time]
    *@Return void
    */
    public void updateStreamingInfo(String complete_time,String next_poll,String records,String temp_cumcumlative,String update_date,String update_time) throws SQLException {
        System.out.println(complete_time+" "+next_poll+" "+records+" "+temp_cumcumlative+" "+update_date + " "+update_time);
        PreparedStatement statement = conn.prepareStatement("INSERT INTO `streaming_info` ( field, `value`, info ) VALUES " +
                "( 'update_date', ?, '更新日期' ), " +
                "( 'update_time', ?, '更新时间' ), " +
                "( 'complete_time', ?, '完成耗时' ), " +
                "( 'records', ?, '记录条数' ), " +
                "( 'next_poll', ?, '下次轮询时间' ), " +
                "( 'temp_cumcumlative', ?, '当前累计条目' ) " +
                "ON DUPLICATE KEY UPDATE field = VALUES(field), `value` =  IF(VALUES(value) is NULL,value,VALUES(value)), info = VALUES(info)");
        statement.setString(1, update_date);
        statement.setString(2,update_time);
        statement.setString(3,complete_time);
        statement.setString(4,records);
        statement.setString(5, next_poll);
        statement.setString(6, temp_cumcumlative);
        statement.executeUpdate();
        conn.commit();
    }

}
