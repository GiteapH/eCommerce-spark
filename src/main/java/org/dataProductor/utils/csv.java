package org.dataProductor.utils;

import org.analysis.utils.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: csv文件工具
 * @date 2023/7/23 14:32
 */
public class csv {
    /**
    *@author 吕杨平
    *@Description 取List,header,生成csv字符串
    *@Date 16:51 2023/7/23
    *@Param [rows, header]
    *@Return java.lang.String
    */

    private static org.apache.log4j.Logger logger = Logger.getInstance();
    public static String toCSVString(List<String[]> rows,boolean add){
        StringBuilder sb = new StringBuilder();
        rows.forEach(row -> {
            for(int i=0;i<row.length;i++){
                sb.append(add?"\"":"").append(row[i]).append(add?"\"":"").append(i == row.length -1 ? "\n": ",");
            }
        });
        return sb.toString();
    }
   /**
   *@author 吕杨平
   *@Description 生成csv文件
   *@Date 17:05 2023/7/23
   *@Param [destFile, rows, header]
   *@Return void
   */
    public static void create(String destFile, List<String[]> rows,String... header){
        BufferedWriter writer = null;
        try {
            File csvFile = new File(destFile);
            File parent = csvFile.getParentFile();
            if(parent != null && !parent.exists()){
                boolean mkdirs = parent.mkdirs();
                if(!mkdirs){
                     logger.error(destFile+"路径创建失败");
                }
            }
            boolean newFile = csvFile. createNewFile();
            if(!newFile){
                logger.error(destFile+"文件创建失败");
            }
            writer = new BufferedWriter(new FileWriter(csvFile));
            writer.write(String.join(",", header)+"\n"+toCSVString(rows,false));
            writer.close();
        }catch (IOException Exception){
            Exception.printStackTrace();
        }
    }
}
