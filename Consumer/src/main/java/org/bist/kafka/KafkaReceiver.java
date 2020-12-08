package org.bist.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class KafkaReceiver {
    protected final String responseTopic;
    public KafkaConsumer<byte[], byte[]> kafkaConsumer;
    ConsumerRecords<byte[], byte[]> consumerRecords;

    boolean correlationFound = false;
    boolean isSegmented = false;
    int segmentCount = 0;
    Integer segmentNumber;

    public KafkaReceiver(String topicName) {
        this.responseTopic = topicName;

        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaContants.KAFKA_SERVER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaContants.CONSUMER_GROUP_NAME);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(KafkaContants.RESPONSE_TOPIC_NAME));
    }

    public Long receive(String correlationId) {
        try {
            while (true) {
                consumerRecords = kafkaConsumer.poll(Duration.of(10, ChronoUnit.NANOS));

                /* ***************************************/
                /*       For one segment response        */
                /*****************************************/
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    isSegmented = false;
                    correlationFound = false;
                    Headers headers = record.headers();
                    Iterator<Header> iterator = headers.iterator();
                    while (iterator.hasNext()) {
                        Header next = iterator.next();
                        if (KafkaContants.CORRELATION_KEY.equals(next.key())) {
                            if (correlationId.equals(new String(next.value()))){
                                correlationFound = true;
                            }
                        }
                        if (KafkaContants.SEGMENT_COUNT.equals(next.key())){
                            isSegmented = true;
                            segmentCount =  (new BigInteger(next.value())).intValue();
                        }

                    }
                    if(correlationFound && !isSegmented)
                        return System.nanoTime();
                    if(correlationFound && isSegmented)
                        break;
                }

                /* ***************************************/
                /*      For multi segment response       */
                /*****************************************/
                if(isSegmented){
                    //segmentCount = 3;
                    System.out.printf("\n Response will be read as segmented " );

                    for (int i = 1; i<=segmentCount; i++){
                        boolean segmentCatched = false;

                        for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                            Headers headers = record.headers();
                            Iterator<Header> iterator = headers.iterator();
                            while (iterator.hasNext()) {
                                Header next = iterator.next();
                                if (KafkaContants.CORRELATION_KEY.equals(next.key())) {
                                    if (correlationId.equals(new String(next.value(),StandardCharsets.UTF_8))){
                                        System.out.printf("\n Record found");
                                        break;
                                    }
                                }
                            }

                            iterator = headers.iterator();
                            while (iterator.hasNext()) {
                                Header next = iterator.next();
                                if (KafkaContants.SEGMENT_NUMBER.equals(next.key())) {
                                    segmentNumber = (new BigInteger(next.value())).intValue();
                                    if (i == segmentNumber.intValue()){
                                        System.out.printf("\n Segment %d is catched !", segmentNumber);
                                        segmentCatched = true;
                                        break;
                                    }
                                }
                            }
                            if(segmentCatched) break;
                        }

                        if(!segmentCatched){
                            consumerRecords = kafkaConsumer.poll(Duration.of(10, ChronoUnit.NANOS));
                            // try to catch same segment
                            i--;
                        }
                    }

                    System.out.printf("\n Read is completed at %d", System.nanoTime());
                    return System.nanoTime();

                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return 0L;
        }
    }

}

