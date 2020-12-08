package org.borsa.kafka;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.KafkaConsumerMetrics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigInteger;
import java.util.Properties;

public class KafkaSender {

    private static final KafkaSender INSTANCE = new KafkaSender();
    private final Producer<String, String> kafkaProducer;

    private KafkaSender() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaContants.KAFKA_SERVER_LIST);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaContants.PRODUCER_IP);
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<String,String>(properties);
    }

    public static KafkaSender getInstance() {
        return INSTANCE;
    }

    public void sendMessage(String topicName, String key, byte[] value, String correlationId) {
        ProducerRecord message = new ProducerRecord(topicName, key, value);
        message.headers().add(KafkaContants.CORRELATION_KEY, correlationId.getBytes());
        kafkaProducer.send(message);
    }

    public void sendMessage(String topicName, String key, byte[] value, Integer segmentNumber, Integer segmentCount, String correlationId) {
        ProducerRecord message = new ProducerRecord(topicName, key, value);
        message.headers().add(KafkaContants.CORRELATION_KEY, correlationId.getBytes());
        message.headers().add(KafkaContants.SEGMENT_NUMBER, byteFromInteger(segmentNumber));
        message.headers().add(KafkaContants.SEGMENT_COUNT, byteFromInteger(segmentCount));
        kafkaProducer.send(message);
    }

    public static byte[] byteFromInteger(Integer integer){
        return BigInteger.valueOf(integer).toByteArray();
    }


}
