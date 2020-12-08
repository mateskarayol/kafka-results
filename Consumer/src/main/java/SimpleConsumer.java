import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class SimpleConsumer {
/*
    public static void main(String[] args){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer< byte[], byte[] > consumer = new KafkaConsumer < > (props);
        consumer.subscribe(Collections.singletonList("simple-topic"));
        while (true) {
            final ConsumerRecords< byte[], byte[] > consumerRecords = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));

            consumerRecords.forEach(record -> {
                System.out.println("Key : " + record.key() + " Value : " + record.value());

                Headers headers = record.headers();
                Iterator<Header> iterator = headers.iterator();
                while (iterator.hasNext()) {
                    Header next = iterator.next();
                    if("correlationId".equals(next.key())){

                        System.out.println("[Consumer]Received message with key: " + record.key() + " value : " + record.value() + " crrID : " + new String(next.value()));
                    }
                }

            });
        }
    }
*/
}
