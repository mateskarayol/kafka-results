package org.borsa.kafka;

import java.util.TimerTask;

public class KafkaTask extends TimerTask {

    public void run(){
        KafkaReceiver receiver = new KafkaReceiver(KafkaContants.REQUEST_TOPIC_NAME);
        receiver.receive();
    }
}

