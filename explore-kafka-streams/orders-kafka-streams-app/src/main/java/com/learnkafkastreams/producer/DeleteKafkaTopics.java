package com.learnkafkastreams.producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DeleteKafkaTopics {
    public static void main(String[] args) {
        String topicName = "orders"; // Replace with your topic name

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(config)) {
            // Delete the topic
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
            System.out.println("Topic deleted: " + topicName);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
