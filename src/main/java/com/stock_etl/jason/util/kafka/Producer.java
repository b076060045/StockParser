package com.stock_etl.jason.util.kafka;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.ProducerRecord; // 絕對不能漏掉這行
import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.Properties;

public class Producer {
    public KafkaProducer<String, String> producer;
    public Producer(String bootstrap_server) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap_server);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public void send(String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    public void close(){
        producer.close();
    }
}
