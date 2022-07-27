package com.flink.mysql;


import com.flink.influxdb.common.ConnectionPoolUtils;
import com.flink.models.Student;
import org.apache.flink.configuration.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/*
* @author Liyan
* create 2022/07/17 21:34
* */
@Slf4j
public class Flink2JdbcWriter extends RichSinkFunction<List<Student>> {
    public static final long serialVersionUID = -5072869539213229634L;

    private transient Connection connection = null;
    private transient PreparedStatement ps = null;
    private transient PreparedStatement up =null;
    private transient PreparedStatement se =null;
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);


        connection = ConnectionPoolUtils.getConnection();

        if (null != connection){
            ps = connection.prepareStatement("insert into student (id, name, age, createDate,create_state) values (?,?,?,?,?);");
            up = connection.prepareStatement("update student SET name=?,age=?,createDate=?,create_state=? where id=?;");
            se = connection.prepareStatement("select create_state from student where id=?;");
        }
    }




    @Override
    public void invoke(List<Student> list, Context context) throws Exception{

        if (isRunning && null != ps){
            for (Student one : list){

                se.setInt(1,one.getId());
                ResultSet resultSet = se.executeQuery();
                if(!resultSet.next()) {
                    ps.setInt(1,one.getId());
                    ps.setString(2, one.getName());
                    ps.setInt(3, one.getAge());
                    ps.setString(4, one.getCreateDate());
                    ps.setString(5, one.getCreatState());
                    ps.addBatch();

                }else {
                    String createState = resultSet.getString("create_state");
                    if (createState != "1") {
                        up.setString(1, one.getName());
                        up.setInt(2, one.getAge());
                        up.setString(3, one.getCreateDate());
                        up.setString(4, one.getCreatState());
                        up.setInt(5, one.getId());
                        up.addBatch();
                    }
                    if (createState == "1") {
                        break;

                    }
                }
            }
            int [] count = ps.executeBatch();
            log.info("成功写入mysql数量："+ count.length);
            int [] count2 = up.executeBatch();
            log.info("成功修改的数据量："+count2.length);
        }
    }


    @Override
    public void close() throws Exception {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (up != null){
                up.close();
            }
            if (se != null){
                se.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        isRunning = false;
    }
}
