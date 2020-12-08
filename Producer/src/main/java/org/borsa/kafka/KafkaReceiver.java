package org.borsa.kafka;

import kafka.Kafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class KafkaReceiver  {

    public KafkaConsumer<byte[], byte[]> kafkaConsumer;

    private static byte[]  responseData;

    private static byte[]  responseData_60 = new byte[60 * 1024];
    private static byte[]  responseData_300 = new byte[300 * 1024];
    private static byte[]  responseData_600 = new byte[600 * 1024];
    private static byte[]  responseData_1024 = new byte[1024 * 1024];

    private BigInteger responseDataSize;
    private String correlationId;

    public static final Integer BYTE_FOR_1_MB = 1_024 * 1_024;

    public KafkaReceiver(String topicName) {

        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaContants.KAFKA_SERVER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaContants.CONSUMER_GROUP_NAME);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(KafkaContants.REQUEST_TOPIC_NAME));
    }


    public void receive() {

        while(true){
            final ConsumerRecords< byte[], byte[] > consumerRecords = kafkaConsumer.poll(Duration.of(10, ChronoUnit.NANOS));

            consumerRecords.forEach(record -> {

                Headers headers = record.headers();
                Iterator<Header> iterator = headers.iterator();
                while (iterator.hasNext()) {
                    Header next = iterator.next();

                    if(KafkaContants.DATA_SIZE_IN_KB.equals(next.key()))
                        responseDataSize = new BigInteger(next.value());

                    if(KafkaContants.CORRELATION_KEY.equals(next.key()))
                        correlationId = new String(next.value());
                }

                if(KafkaContants.TEST_TYPE.equals(KafkaContants.TEST_TYPE_QUERY)){
                    if(responseDataSize.intValue() == 60 )
                        responseData = responseData_60;
                    else if(responseDataSize.intValue() == 300 )
                        responseData = responseData_300;
                    else if(responseDataSize.intValue() == 600 )
                        responseData = responseData_600;

                    // Send segmented response segment size -> 1024 megabyte
                    if( responseDataSize.intValue() * 1024 > BYTE_FOR_1_MB ){
                        responseData = responseData_1024;
                        int segmentCount = responseDataSize.intValue() * 1024/BYTE_FOR_1_MB;

                        for( int segmentNumber = 1; segmentNumber <= segmentCount; segmentNumber++)
                            KafkaSender.getInstance().sendMessage(KafkaContants.RESPONSE_TOPIC_NAME, "responseKey", responseData, segmentNumber, segmentCount, correlationId);


                    }else{
                        KafkaSender.getInstance().sendMessage(KafkaContants.RESPONSE_TOPIC_NAME, "responseKey", responseData, correlationId);
                    }

                }else{
                    KafkaSender.getInstance().sendMessage(KafkaContants.RESPONSE_TOPIC_NAME, "responseKey", "0".getBytes(), correlationId);
                }
            });
        }
    }

}
