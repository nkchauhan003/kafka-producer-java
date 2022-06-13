package com.cb.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DemoProducerImpl implements DemoProducer<String, String> {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "demo-producer";

    public DemoProducerImpl(String topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(properties);
        this.topic = topic;
    }

    @Override
    public void produce(String key, String value) throws ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        RecordMetadata metadata = producer.send(new ProducerRecord<>(
                topic,
                key,
                value)).get();
        System.out.println(
                "Message with id: '" + key + "' sent to partition(" + metadata.partition() + "), offset(" + metadata.offset() + ") in " + (System.currentTimeMillis() - startTime) + " ms");
    }

    @Override
    public void produceAsync(String key, String value) {
        long startTime = System.currentTimeMillis();
        producer.send(new ProducerRecord<>(
                topic,
                key,
                value), new DemoCallBack(startTime, key, value));
    }

    private class DemoCallBack implements Callback {

        private final long startTime;
        private final String key;

        public DemoCallBack(long startTime, String key, String value) {
            this.startTime = startTime;
            this.key = key;
        }

        /**
         * called when the record sent to the Kafka Server has been acknowledged.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metadata != null) {
                System.out.println(
                        "Message with id: '" + key + "' sent to partition(" + metadata.partition() + "), offset(" + metadata.offset() + ") in " + (System.currentTimeMillis() - startTime) + " ms");
            } else {
                exception.printStackTrace();
            }
        }
    }
}
