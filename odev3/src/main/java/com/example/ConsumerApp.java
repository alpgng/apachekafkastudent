package com.example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerApp {
    public static void main(String[] args) {
        // Kafka consumer settings
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "student-consumer-group");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("students")); // Subscribing to the "students" topic

        // Poll for new messages and print them
        while (true) {
            consumer.poll(100).forEach(record -> {
                System.out.println("Received event: " + record.value());
            });
        }
    }
}
