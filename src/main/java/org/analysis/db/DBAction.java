package org.analysis.db;

import org.analysis.utils.Logger;
import org.analysis.utils.ProgressUtil;
import org.analysis.utils.commonAnalysisUtils;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.*;
import java.util.Arrays;
import java.util.Iterator;

public class DBAction implements Serializable {
    static org.apache.log4j.Logger logger = Logger.getInstance();
    public static Connection conn;
    static {
        try {
            conn = new Conn().getConn();
            conn.setAutoCommit(false);
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    public void select(String sql){
        try {
            Statement statement = conn.createStatement();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void insertCountFull(Dataset<Row> count) throws SQLException {
        count.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement preparedStatement = conn.prepareStatement("replace into act_type_count values (?,?,?,?)");
                int count = 0;
                while (iterator.hasNext()) {
                    Row next = iterator.next();
                    preparedStatement.setString(1, next.getAs("address"));
                    preparedStatement.setLong(2, next.getAs("count"));
                    preparedStatement.setInt(3, next.getAs("act_type"));
                    preparedStatement.setInt(4, next.getAs("sku"));
                    preparedStatement.addBatch();
                    if(count%3000==0){
                        preparedStatement.executeBatch();
                        conn.commit();
                        preparedStatement.clearBatch();
                    }
                    count ++;
                }
                preparedStatement.executeBatch();
                conn.commit();
            }
        });
    }

    public void insertSkuPrice(Dataset<Row> skuPrice) throws SQLException {
        skuPrice.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement preparedStatement = conn.prepareStatement("replace into sku_price values (?,?,?,?)");
                int count = 0;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    preparedStatement.setInt(1,row.getAs("sku"));
                    preparedStatement.setDouble(2, row.getAs("sum_price"));
                    preparedStatement.setLong(3,row.getAs("num"));
                    preparedStatement.setString(4,row.getAs("address"));
                    preparedStatement.addBatch();
                    if (count % 3000 == 0) {
                        preparedStatement.executeBatch();
                        conn.commit();
                        preparedStatement.clearBatch();
                    }
                    count++;
                }
                preparedStatement.executeBatch();
                conn.commit();
            }
        });
    }
    
    public void insertCentralizedDistribution(Dataset<Row> centralizedDistributionDataset){
        centralizedDistributionDataset.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement preparedStatement = conn.prepareStatement("insert into centralized_distribution values (?,?,?,?,?,?,?,?,?,?,?,?)");
                int count = 0;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    try{
                        long[] centralizedDistributionVals = commonAnalysisUtils.getCentralizedDistributionVals(row);
                        for(int i=0;i<centralizedDistributionVals.length;i++){
                            preparedStatement.setLong(i+2,centralizedDistributionVals[i]);
                        }
                        preparedStatement.setInt(1,row.getAs("sku"));
                        preparedStatement.setString(9,row.getAs("address"));
                        preparedStatement.setString(10,row.getAs("province"));
                        preparedStatement.setString(11,row.getAs("city"));
                        preparedStatement.setString(12,row.getAs("county"));
                        preparedStatement.addBatch();
                        if(count%3000==0){
                            preparedStatement.executeBatch();
                            conn.commit();
                            preparedStatement.clearBatch();
                        }
                        count++;
                    }catch (SQLException e) {
                        e.printStackTrace();
                        throw e;
                    }
                }
                preparedStatement.executeBatch();
                conn.commit();
            }
        });
    }

    public void insertRepurchase(Dataset<Row> repurchase) throws SQLException {
        repurchase.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement preparedStatement = conn.prepareStatement("replace into repurchase values (?,?,?,?,?,?)");
                int count = 0;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    try {
                        preparedStatement.setInt(1, row.getAs("repurchase"));
                        preparedStatement.setString(2, row.getAs("address"));
                        preparedStatement.setInt(3, row.getAs("sku"));
                        preparedStatement.setInt(4, row.getAs("id"));
                        preparedStatement.setLong(5, row.getAs("purchase_num"));
                        preparedStatement.setString(6,row.getAs("act_date"));
                        preparedStatement.addBatch();
                        if (count % 3000 == 0) {
                            preparedStatement.executeBatch();
                            conn.commit();
                            preparedStatement.clearBatch();
                        }
                        count++;
                    }catch (SQLException e) {
                        e.printStackTrace();
                        logger.error(row);
                    }
                }
                preparedStatement.executeBatch();
                conn.commit();
            }
        });
    }

    public void insertBaseTag(Dataset<Row> baseTags) throws SQLException {
        ProgressUtil pu = new ProgressUtil("insert baseTag (插入数据统计标签中)",baseTags.count());
        baseTags.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement preparedStatement = conn.prepareStatement("replace into base_user_tag values (?,?,?,?,?,?)");
                int count = 1;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    try {
                        preparedStatement.setInt(1,row.getAs("user_id"));
                        preparedStatement.setString(2,row.getAs("gender"));
                        preparedStatement.setLong(3,row.getAs("age"));
                        preparedStatement.setString(4,row.getAs("province"));
                        preparedStatement.setString(5,row.getAs("city"));
                        preparedStatement.setString(6,row.getAs("county"));
                        preparedStatement.addBatch();
                        if(count % 5000==0){
                            preparedStatement.executeBatch();
                            preparedStatement.clearBatch();
                            conn.commit();
                            pu.finishOne("完成量"+count);
                        }
                        count++;
                    } catch (Exception e) {
                        pu.finishOne("失败:"+row);
                    }
                }
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                conn.commit();
            }
        });
    }

    public void insertActCount(Dataset<Row> user_act_count) throws SQLException {
        ProgressUtil pu = new ProgressUtil("insert act_count(插入用户行为计数)", user_act_count.count());
        user_act_count.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                int count=1;
                PreparedStatement preparedStatement = conn.prepareStatement("replace into user_act_count values (?,?,?,?)");
                while(iterator.hasNext()){
                    Row row = iterator.next();
                    preparedStatement.setInt(1, row.getAs("user"));
                    preparedStatement.setInt(2, row.getAs("act_type"));
                    preparedStatement.setLong(3, row.getAs("count"));
                    preparedStatement.setString(4, row.getAs("address"));
                    preparedStatement.addBatch();
                    if(count%3000==0){
                        preparedStatement.executeBatch();
                        conn.commit();
                        preparedStatement.clearBatch();
                    }
                    count++;
                }
                preparedStatement.executeBatch();
                conn.commit();
            }
        });
    }

    public void insertJumpLoss(Dataset<Row> jumpLoss){
        jumpLoss.show();
        jumpLoss.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                int count = 1;
                PreparedStatement preparedStatement = conn.prepareStatement("replace into sku_jump_loss values (?,?,?)");
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    preparedStatement.setInt(1,row.getAs("sku"));
                    preparedStatement.setInt(2,row.getAs("watch"));
                    preparedStatement.setLong(3,row.getAs("watch_num"));
                    preparedStatement.addBatch();
                    if(count%3000==0){
                        preparedStatement.executeBatch();
                        conn.commit();
                        preparedStatement.clearBatch();
                    }
                    count++;
                }
                preparedStatement.executeBatch();
                conn.commit();
            }
        });
    }
}
