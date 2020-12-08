import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class SimpleProducer {
/*
     public static void main(String[] args) {

        Properties props = new Properties();

        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("max.block.ms", "3000");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        TestCallback callback = new TestCallback();
        int counter = 100;
        while(true)
            try(KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                ProducerRecord<String, String> record = new ProducerRecord<>("simple-topic","key" + counter ,"message" + counter);

                record.headers().add("correlationId", ("crr" + counter).getBytes());
;               producer.send(record, callback);

                System.out.printf("sent record(key=%s value=%s) \n", record.key(), record.value());
                counter++;

            } catch (Exception e) {
                System.out.println(" Exception occured !!! ");
                System.out.println(e.getMessage());
            }

        }
*/
    }

    class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset());
                System.out.println(message);
            }
        }
    }

