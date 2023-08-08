package org.analysis.db;

import org.analysis.Enum.ruleTableEnum;
import org.analysis.utils.ProgressUtil;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import static org.analysis.db.DBAction.conn;

public class ruleDBAction implements Serializable {

    public void insertRuleTag(Dataset<Row> ruleGroup, int time) throws SQLException {
        ProgressUtil pu = new ProgressUtil("insert ruleTags(插入规则标签中)", ruleGroup.count()/1000);
        ruleGroup.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement ruleStatement = conn.prepareStatement(String.format("replace into %s values (?,?,?,?,?,?,?,?,?,?,?,?,?)",ruleTableEnum.getRuleUserTagByTime(time).tableName));
                int i = 1;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    try {
                        ruleStatement.setInt(1, row.getAs("id"));
                        ruleStatement.setString(2, row.getAs("max_time_num"));
                        ruleStatement.setLong(3, row.getAs("max_time"));
                        ruleStatement.setString(4, row.getAs("max_price_num"));
                        ruleStatement.setLong(5, row.getAs("max_price"));
                        ruleStatement.setLong(6, row.getAs("type_1"));
                        ruleStatement.setLong(7, row.getAs("type_2"));
                        ruleStatement.setLong(8, row.getAs("type_3"));
                        ruleStatement.setLong(9, row.getAs("type_4"));
                        ruleStatement.setLong(10, row.getAs("type_5"));
                        ruleStatement.setLong(11, row.getAs("type_6"));
                        ruleStatement.setLong(12, row.getAs("type_7"));
                        ruleStatement.setString(13, row.getAs("address"));
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

    public void insertTimeSplit(Dataset<Row> ts) throws SQLException {
        final long[] count = {0};
        ProgressUtil pu = new ProgressUtil("insert time_split(插入时间分布中)", ts.count());
        PreparedStatement timeSplitStatement = conn.prepareStatement("replace into user_preference values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        ts.foreach((ForeachFunction<Row>) row -> {
            try {
                timeSplitStatement.setInt(1, row.getAs("id"));
                timeSplitStatement.setLong(2, row.getAs("dawn"));
                timeSplitStatement.setLong(3, row.getAs("morning"));
                timeSplitStatement.setLong(4, row.getAs("noon"));
                timeSplitStatement.setLong(5, row.getAs("midday"));
                timeSplitStatement.setLong(6, row.getAs("afternoon"));
                timeSplitStatement.setLong(7, row.getAs("evening"));
                timeSplitStatement.setLong(8, row.getAs("low"));
                timeSplitStatement.setLong(9, row.getAs("super_low"));
                timeSplitStatement.setLong(10, row.getAs("medium"));
                timeSplitStatement.setLong(11, row.getAs("above_moderate"));
                timeSplitStatement.setLong(12, row.getAs("finest"));
                timeSplitStatement.setLong(13, row.getAs("higher"));
                timeSplitStatement.setLong(14, row.getAs("highest"));
                timeSplitStatement.executeUpdate();
                pu.finishOne("完成量" + (count[0]++));
            } catch (Exception normal) {
                normal.printStackTrace();
                pu.failureOne("失败量" + row);
            }
        });
    }


    public void insertTransactionInterval(Dataset<Row> transactionInterval) throws SQLException {
        ProgressUtil pu = new ProgressUtil("insert transaction_interval(插入交易间隔)", 26623752);
        transactionInterval.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement preparedStatement = conn.prepareStatement("replace into transaction_interval values (?,?,?,?)");
                int count = 1;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    preparedStatement.setInt(1, row.getAs("user"));
                    preparedStatement.setString(2, row.getAs("act_date"));
                    preparedStatement.setString(3, row.getAs("act_time"));
                    preparedStatement.setInt(4, row.getAs("diff"));
                    preparedStatement.addBatch();
                    if (count % 4000 == 0) {
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



    public void insertUserIdentity(Dataset<Row> lossDataset, Dataset<Row> backDataset) throws SQLException {
        lossDataset.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement preparedStatement1 = conn.prepareStatement("replace into user_identity values (?,?,?)");
                int count = 1;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    preparedStatement1.setInt(1, row.getAs("user"));
                    preparedStatement1.setInt(2, 0);
                    preparedStatement1.setInt(3, 1);
                    preparedStatement1.addBatch();
                    if(count % 3000==0) {
                        preparedStatement1.executeBatch();
                        conn.commit();
                        preparedStatement1.clearBatch();
                    }
                    count++;
                }
                preparedStatement1.executeBatch();
                conn.commit();
            }
        });
        backDataset.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement preparedStatement2 = conn.prepareStatement("replace into user_identity values (?,?,?)");
                int count = 1;
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    preparedStatement2.setInt(1, row.getAs("user"));
                    preparedStatement2.setInt(2, 0);
                    preparedStatement2.setInt(3, 1);
                    preparedStatement2.addBatch();

                    if(count%3000==0){
                        preparedStatement2.executeBatch();
                        conn.commit();
                        preparedStatement2.clearBatch();
                    }
                    count++;
                }
                preparedStatement2.executeBatch();
                conn.commit();
            }
        });
    }


//    插入累计交易额和时间分布
    public void insertSplitter(Dataset<Row> splitter){
        splitter.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                int count = 1;
                PreparedStatement preparedStatement = conn.prepareStatement("replace into user_centralized_distribution values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    preparedStatement.setInt(1, row.getAs("id"));
                    preparedStatement.setString(2, row.getAs("address"));
                    preparedStatement.setLong(3, row.getAs("dawn"));
                    preparedStatement.setLong(4, row.getAs("morning"));
                    preparedStatement.setLong(5, row.getAs("noon"));
                    preparedStatement.setLong(6, row.getAs("midday"));
                    preparedStatement.setLong(7, row.getAs("afternoon"));
                    preparedStatement.setLong(8, row.getAs("evening"));
                    preparedStatement.setLong(9, row.getAs("low"));
                    preparedStatement.setLong(10, row.getAs("super_low"));
                    preparedStatement.setLong(11, row.getAs("medium"));
                    preparedStatement.setLong(12, row.getAs("above_moderate"));
                    preparedStatement.setLong(13, row.getAs("finest"));
                    preparedStatement.setLong(14, row.getAs("higher"));
                    preparedStatement.setLong(15, row.getAs("highest"));

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
