package org.borsa.kafka;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;

public class MainProducer {

    public static DateFormat timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args)  {

        System.out.printf(" Server Parameters : \n " +
                        " Start time            : %s \n" +
                        " Test type             : %s \n" +
                        " Kafka Server List     : %s \n" +
                        " Producer Server List  : %s \n\n ",
                        KafkaContants.START_TIME,
                        KafkaContants.TEST_TYPE,
                        KafkaContants.KAFKA_SERVER_LIST,
                        KafkaContants.PRODUCER_IP );

        try{
            Date date = timeFormatter.parse(KafkaContants.START_TIME);
            Timer timer = new Timer();
            timer.schedule(new KafkaTask(), date);

        }catch(ParseException pe){
            System.out.println(pe.getMessage());
        }

    }

}
