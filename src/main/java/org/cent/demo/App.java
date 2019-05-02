package org.cent.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A demo of using kafka-clients producer
 *
 */
public class App 
{
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main( String[] args ) throws InterruptedException {
        logger.info("Go testing kafka producer");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        logger.info("create a producer to send some message");

        String msg = "message %d created at %s";
        String value;
        for(int i=0; i<10; i++) {
            value = String.format(msg, i, new Timestamp(System.currentTimeMillis()));
            logger.info("make a message: " + value);
            producer.send(new ProducerRecord<>("test", Integer.toString(i), value));

            TimeUnit.SECONDS.sleep(3);
        }

        logger.info("End testing and closing the producer");
        producer.close();
    }
}
