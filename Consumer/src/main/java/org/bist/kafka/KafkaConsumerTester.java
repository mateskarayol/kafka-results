package org.bist.kafka;

import org.bist.statistics.Statistics;

import java.util.UUID;

public class KafkaConsumerTester {

    public static void test(byte[] reqDataSize, int repeatCount, Statistics statistics){

        KafkaSender sender = new KafkaSender();
        KafkaReceiver receiver = new KafkaReceiver(KafkaContants.RESPONSE_TOPIC_NAME);
        UUID uniqueCorrelationId;
        Long startNano, endNano;
        Integer counter = 0;

        do{
            uniqueCorrelationId = UUID.randomUUID();
            startNano = sender.sendMessage(KafkaContants.REQUEST_TOPIC_NAME, "requestKey" ,"" , uniqueCorrelationId.toString() , reqDataSize);
            endNano = receiver.receive(uniqueCorrelationId.toString() );

            statistics.transactionCompleted(startNano, endNano);
            counter++;

        }while(counter < repeatCount);

        System.out.println("\n" + statistics.getSummary());
    }
}
