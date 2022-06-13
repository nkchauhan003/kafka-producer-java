package com.cb;

import com.cb.producer.DemoProducer;
import com.cb.producer.DemoProducerImpl;

import java.util.Random;
import java.util.concurrent.ExecutionException;


/**
 * Hello world!
 */
public class App {
    private static final String TOPIC = "demo-topic";

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        DemoProducer<String, String> producer = new DemoProducerImpl(TOPIC);
        Random random = new Random();
        int i = 0;
        while (true) {
            Thread.sleep(1000);
            producer.produceAsync(String.valueOf(random.nextInt(50)), "Message:" + i++);
        }
    }
}
