package com.flink.influxdb.common;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import lombok.extern.slf4j.Slf4j;


import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


/*
 * @author Iliyan
 * @create 2022/07/17 17:40
 *  @*/

@Slf4j
public class ConnectionPoolUtils {
    private static final Properties properties;
    private static Connection conn;

    static {
        properties = new Properties();
        ClassLoader classLoader = ConnectionPoolUtils.class.getClassLoader();
        InputStream resourceAsStream = classLoader.getResourceAsStream("jdbc.properties");

//        加载配置文件
        try {
            properties.load(resourceAsStream);
        } catch (IOException e){
            log.error("Load jdbc properties exception");
        }
    }


    private ConnectionPoolUtils(){

    }

    public static Connection getConnection() throws SQLException{
        try {
            DataSource dataSource = DruidDataSourceFactory.createDataSource(properties);
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            log.error("error occurred: "+ e);
            conn = null;
        }catch (Exception e){
            log.error("error occurred while creating connection pool !");
        }
        return conn;
    }
}