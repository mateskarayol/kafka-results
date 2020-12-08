package org.bist.kafka;

public class KafkaContants {

    public static final String RESPONSE_TOPIC_NAME = System.getProperty("kafka.response.topic", "reply-topic");
    public static final String REQUEST_TOPIC_NAME = System.getProperty("kafka.request.topic", "query-topic");
    public static final String CLIENT_ID = System.getProperty("client.id", "0");

    public static final String KAFKA_SERVER_LIST = System.getProperty("kafka.broker.address", "127.0.0.1:9092");
    public static final String CONSUMER_IP = System.getProperty("kafka.consumer.address", "127.0.0.1:9092");
    public static final String CONSUMER_GROUP_NAME = System.getProperty("kafka.consumer.group", "test-consumer-group");

    public static final String START_TIME = System.getProperty("kafka.start.time", "2020-12-01 12:00:00");
    public static final String NUMBER_OF_CLIENT = System.getProperty("kafka.numberof.client", "1");
    public static final String NUMBER_OF_SERVER = System.getProperty("kafka.numberof.server", "1");
    public static final String REPEAT_COUNT = System.getProperty("kafka.repeat.count", "1000");
    public static final String SIZE_IN_BYTES = System.getProperty("kafka.data.size", "60");
    public static final String WARM_UP = System.getProperty("kafka.warmup.run", "true");

    public static final String TEST_TYPE = System.getProperty("kafka.test.type", "Q");

    public static final String CORRELATION_KEY = "correlationId";
    public static final String SEGMENT_NUMBER = "segmentNumber";
    public static final String SEGMENT_COUNT = "segmentCount";
    public static final String DATA_SIZE_IN_KB = "dataSizeInKb";

}

