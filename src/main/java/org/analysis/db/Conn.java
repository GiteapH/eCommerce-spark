package org.analysis.db;


import org.analysis.utils.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
public class Conn implements Serializable {
    org.apache.log4j.Logger logger = Logger.getInstance();
    String driver=Config_Mysql.getInstance().getString("jdbc.driver.class");
    String url=Config_Mysql.getInstance().getString("jdbc.connection.url");
    String username=Config_Mysql.getInstance().getString("jdbc.connection.username");
    String password=Config_Mysql.getInstance().getString("jdbc.connection.password");


    public Connection getConn(){
        try{
            // 载入数据库驱动
            Class.forName(driver);
            // 建立数据库连接getconnection(jdbc:mysql://地址:端口号/数据库名,数据库用户名，密码)
            System.out.println(username+" : " +password+" : "+url);
            return DriverManager.getConnection(url, username, password);
        }catch (SQLException | ClassNotFoundException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
