package com.learnkafkastreams;


import com.learnkafkastreams.topology.OrdersTopology;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


@Slf4j
public class OrdersKafkaStreamApp {


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // create an instance of the topology

        var topology = new OrdersTopology().buildTopology();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-app"); // consumer group
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimeStampExtractor.class);
        // read only the new messages
        AdminClient admin = AdminClient.create(config);
        //admin.deleteTopics(Collections.singletonList("general_orders")).all().get();
        //admin.deleteTopics(Collections.singletonList("orders")).all().get();
        //admin.deleteTopics(Collections.singletonList("restaurant_orders")).all().get();


        createTopics(config, List.of(OrdersTopology.STORES, OrdersTopology.GENERAL_ORDERS,OrdersTopology.RESTAURANT_ORDERS,OrdersTopology.ORDERS), admin);

        //Create an instance of KafkaStreams
        var kafkaStreams = new KafkaStreams(topology, config);

        //This closes the streams anytime the JVM shuts down normally or abruptly.
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        try{
            kafkaStreams.start();
        }catch (Exception e ){
            log.error("Exception in starting the Streams : {}", e.getMessage(), e);
        }

    }

    private static void createTopics(Properties config, List<String> greetings, AdminClient admin) {

        //AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication  = 1;

        var newTopics = greetings
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }

}
