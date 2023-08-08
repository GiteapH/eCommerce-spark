package org.analysis.db;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import static org.analysis.db.DBAction.conn;

public class rfmDBAction implements Serializable {
    public void insertrfm(Dataset<Row> rfm,int time) throws SQLException {
        rfm.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement preparedStatement = conn.prepareStatement("replace into rfm values (?,?,?,?,?,?,?,?,?,?,?,?,?)");
                int count = 1;
                while(iterator.hasNext()) {
                    Row row = iterator.next();
                    preparedStatement.setInt(1,row.getAs("user"));
                    preparedStatement.setString(2,row.getAs("consumption_capacity_score_tag"));
                    preparedStatement.setString(3,row.getAs("recency_score_tag"));
                    preparedStatement.setString(4, row.getAs("frequency_score_tag"));
                    preparedStatement.setString(5, row.getAs("rfm_tag"));
                    preparedStatement.setInt(6,time);
                    preparedStatement.setDouble(7, row.getAs("consumption_capacity"));
                    preparedStatement.setInt(8, row.getAs("recency"));
                    preparedStatement.setLong(9, row.getAs("frequency"));
                    preparedStatement.setInt(10,row.getAs("consumption_capacity_score"));
                    preparedStatement.setInt(11, row.getAs("recency_score"));
                    preparedStatement.setInt(12,row.getAs("frequency_score"));
                    preparedStatement.setString(13, row.getAs("rfm_address"));
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
