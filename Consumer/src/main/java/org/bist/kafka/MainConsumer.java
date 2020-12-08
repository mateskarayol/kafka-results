package org.bist.kafka;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;

public class MainConsumer {

    public static DateFormat timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String [] args ){

        System.out.printf(" Server Parameters : \n " +
                " Number of client      : %s \n" +
                " Number of server      : %s \n" +
                " Start time            : %s \n" +
                " Test type             : %s \n" +
                " Repeat Count          : %s \n" +
                " Size in Bytes         : %s \n" +
                " Warm up               : %s \n" +
                " Kafka Server List     : %s \n" +
                " Consumer Server List  : %s \n\n ",
                KafkaContants.NUMBER_OF_CLIENT,
                KafkaContants.NUMBER_OF_SERVER,
                KafkaContants.START_TIME,
                KafkaContants.TEST_TYPE,
                KafkaContants.REPEAT_COUNT,
                KafkaContants.SIZE_IN_BYTES,
                KafkaContants.WARM_UP,
                KafkaContants.KAFKA_SERVER_LIST,
                KafkaContants.CONSUMER_IP );

        try{
            Date date = timeFormatter.parse(KafkaContants.START_TIME);
            Timer timer = new Timer();
            timer.schedule(new KafkaTask(), date);

        }catch(ParseException pe){
            System.out.println(pe.getMessage());
        }

    }

}