package com.flink;

import com.alibaba.fastjson.JSON;
import com.flink.models.Student;
import com.flink.mysql.Flink2JdbcWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;



import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;


import java.util.List;

/* @author Liyan
*  @createData 2022/07/17 22:06
* */
@Slf4j
public class FlinkReadDbWriterDb {
    public static void main(String[] args) throws Exception {

//        创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        kafka配置
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                设置订阅的目标主题
                .setTopics("Student")
//                设置消费者组
                .setGroupId("Student01")
//                设置kafka服务地址
                .setBootstrapServers("39.106.11.207:9092")
//                设置偏移量
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
//                设置反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())
//                开启kafka底层消费者的自动位移提交机制
                .setProperty("auto.offset.commit", "true")
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk-source");
        DataStream<Student> dataStream = dataStreamSource.map(string -> JSON.parseObject(string, Student.class));


//        收集5秒内的总数据
        dataStream.timeWindowAll(Time.seconds(5L)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Student> iterable, Collector<List<Student>> collector) throws Exception {
                List<Student> students = Lists.newArrayList(iterable);
                if (CollectionUtils.isNotEmpty(students)) {
                    log.info("5秒总共接收的条数：" + students.size());
                    collector.collect(students);
                }
            }
            }).addSink(new Flink2JdbcWriter());



        env.execute("Flink Streaming java API Skeleton");


    }
}
