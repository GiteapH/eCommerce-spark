package org.analysis.db;
import java.io.Serializable;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
// 读取数据库配置文件sql.properties类
public class Config_Mysql implements Serializable {


    private static Config_Mysql config_mysql;
    private static Properties properties;
    private Config_Mysql(){
        String configFile="sql.properties"; // 数据库配置文件
        properties = new Properties();
        InputStream is = Config_Mysql.class.getClassLoader().getResourceAsStream(configFile);
        try{
            properties.load(is);
            is.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public static Config_Mysql getInstance(){

        if(config_mysql == null){
            config_mysql=new Config_Mysql();
        }
        return config_mysql;
    }

    // 通过配置文件Key的名称获取到Key的值。
    public String getString(String key){
        return properties.getProperty(key);
    }


}