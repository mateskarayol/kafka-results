package org.bist.kafka;

import org.bist.statistics.LatencyStatistics;
import org.bist.statistics.Statistics;

import java.io.IOException;
import java.math.BigInteger;
import java.util.TimerTask;

public class KafkaTask extends TimerTask {

    public void run(){

        Statistics statistics_warmup;
        Statistics statistics_test;

        Integer reqDataSize = Integer.valueOf(KafkaContants.SIZE_IN_BYTES);
        byte[] reqDataSizeInByte = byteArrFromInteger(reqDataSize);
        Boolean warmUp = Boolean.parseBoolean(KafkaContants.WARM_UP);
        Integer repeatCount = Integer.valueOf(KafkaContants.REPEAT_COUNT);

        try {
            if(warmUp){
                System.out.println("Warm Up Is Running------------------------------------------------------");
                statistics_warmup = new LatencyStatistics(10);
                KafkaConsumerTester.test(reqDataSizeInByte, repeatCount/10, statistics_warmup);
                System.out.printf("\nWarm up set completed  for size :  %d  repeat count : %d", reqDataSize, repeatCount/10);
                String reportFilename = String.format("Warm-%s-%s-%s-%dkb", KafkaContants.TEST_TYPE, KafkaContants.NUMBER_OF_SERVER, KafkaContants.NUMBER_OF_CLIENT, reqDataSize);
                statistics_warmup.dumpRawData(reportFilename);
            }

            statistics_test = new LatencyStatistics(10);
            KafkaConsumerTester.test(reqDataSizeInByte, repeatCount, statistics_test);
            System.out.printf("\nSummary : %s", statistics_test.getSummary());
            String reportFilename = String.format("Kafka-%s-%s-%s-%dkb-%s", KafkaContants.TEST_TYPE, KafkaContants.NUMBER_OF_SERVER, KafkaContants.NUMBER_OF_CLIENT, reqDataSize, KafkaContants.CLIENT_ID);
            statistics_test.dumpRawData(reportFilename);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static byte[] byteArrFromInteger(Integer integer){
        return BigInteger.valueOf(integer).toByteArray();
    }
}
