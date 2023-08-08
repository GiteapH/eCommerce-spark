package org.analysis.rfm.db;

import org.analysis.Enum.ruleTableEnum;
import org.analysis.utils.ProgressUtil;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import static org.analysis.db.DBAction.conn;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: 插入并更新规则
 * @date 2023/7/26 11:52
 */
public class ruleDBAction {
    /**
    *@author 吕杨平
    *@Description 插入并更新用户累计行为次数
    *@Date 12:00 2023/7/26
    *@Param [ruleGroup, time]
    *@Return void
    */
    public void insertRuleTag(Dataset<Row> ruleGroup, int time) throws SQLException {
        ProgressUtil pu = new ProgressUtil("insert ruleTags(插入规则标签中)", ruleGroup.count()/1000);
        ruleGroup.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                PreparedStatement ruleStatement = conn.prepareStatement(String.format("replace into %s values (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE cumulative_purchases=VALUES(cumulative_purchases)+?," +
                        "cumulative_cart=cumulative_cart+?," +
                        "cumulative_view=cumulative_view+?," +
                        "cumulative_watch=cumulative_watch+?," +
                        "cumulative_inquire=cumulative_inquire+?," +
                        "cumulative_comment=cumulative_comment+?," +
                        "cumulative_complaint=cumulative_complaint+?", ruleTableEnum.getRuleUserTagByTime(time).tableName));
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
                        ruleStatement.setLong(14, row.getAs("type_1"));
                        ruleStatement.setLong(15, row.getAs("type_2"));
                        ruleStatement.setLong(16, row.getAs("type_3"));
                        ruleStatement.setLong(17, row.getAs("type_4"));
                        ruleStatement.setLong(18, row.getAs("type_5"));
                        ruleStatement.setLong(19, row.getAs("type_6"));
                        ruleStatement.setLong(20, row.getAs("type_7"));
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
}
