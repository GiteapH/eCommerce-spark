package org.analysis.db;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;

import java.sql.PreparedStatement;
import java.util.Iterator;

import static org.analysis.db.DBAction.conn;

/**
 * @author Administrator
 * @version 1.0
 * @description: TODO
 * @date 2023/5/24 13:55
 */
public class clvDBAction {
   public void insertClv(Dataset<Row> clvsDataset){
       clvsDataset.foreachPartition(new ForeachPartitionFunction<Row>() {
           @Override
           public void call(Iterator<Row> iterator) throws Exception {
               PreparedStatement preparedStatement = conn.prepareStatement("replace into clv_forecast values (?,?,?,?,?,?)");
               while (iterator.hasNext()) {
                   Row row = iterator.next();
                   preparedStatement.setInt(0,row.getAs("user"));
                   preparedStatement.setString(1,row.getAs("user_value"));
//                   TODO
               }
           }
       });
   }
}
