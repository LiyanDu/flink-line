package com.flink.kafka;

/*
* @author Liyan
* @create 2022/07/19 00:03
* */

import com.alibaba.fastjson.JSON;
import com.flink.models.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;




@Slf4j
public class KafkaWriter {
//     本地机器kafka列表
    public static final String BROKER_LIST = "39.106.11.207:9092";

//    kafka的topic
    public static final String TOPIC_PERSON = "student";
//    key的序列化的方式，采用字符串的形式
    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
//    value的序列化法师，采用字符串
    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void writeToKafka(){
        Properties properties = new Properties();
            properties.put("bootstrap.servers", BROKER_LIST);
        properties.put("key.serializer", KEY_SERIALIZER);
        properties.put("value.serializer", VALUE_SERIALIZER);

        try (KafkaProducer<String,String> producer = new KafkaProducer<>(properties)){
//构建person对象，在name为hqs后面加随机数
            int randomInt = RandomUtils.nextInt(1,10000);
            int randomId = RandomUtils.nextInt(1,10);
            String randomState = String.valueOf(RandomUtils.nextInt(0,2));
            Student student = new Student();
            student.setId(randomId);
            student.setName("nandy" + randomInt);
            student.setAge(randomInt);
            student.setCreateDate(LocalDateTime.now().toString());
            student.setCreatState(randomState);

//            转换为JSON
            String personJson = JSON.toJSONString(student);
//            包装成kafka发送记录
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_PERSON, null, null, personJson);

//           发送到缓存
            producer.send(record);
            log.info("向kafka发送数据：" + personJson);
//            立即发送
            producer.flush();
        }
    }

    public static void main(String[] args) {
        int count = 0;
        while (count < 20){
            try {
//                每三秒发送一条
                TimeUnit.SECONDS.sleep(3);
                writeToKafka();
                count++;
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
