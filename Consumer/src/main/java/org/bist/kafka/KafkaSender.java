package org.bist.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaSender {

    private static final KafkaSender INSTANCE = new KafkaSender();
    private final Producer<String, String> kafkaProducer;

    public KafkaSender() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaContants.KAFKA_SERVER_LIST);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaContants.CONSUMER_IP);
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    public static KafkaSender getInstance() {
        return INSTANCE;
    }

    public Long sendMessage(String topicName, String key, String value, String correlationId, byte[] reqDataSize) {

        ProducerRecord message = new ProducerRecord(topicName, key, value );
        message.headers().add(KafkaContants.DATA_SIZE_IN_KB, reqDataSize);
        message.headers().add(KafkaContants.CORRELATION_KEY, (correlationId).getBytes());

        //System.out.printf("\n QUERY IS SEND  KEY : %s VALUE : %s CRRID : %s \n", message.key(), message.value(), correlationId);
        Long nanoTime = System.nanoTime();
        kafkaProducer.send(message);
        return nanoTime;
    }

}


