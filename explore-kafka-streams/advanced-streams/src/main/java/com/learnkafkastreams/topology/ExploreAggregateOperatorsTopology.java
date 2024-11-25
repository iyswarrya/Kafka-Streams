package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.AlphabetWordAggregate;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreAggregateOperatorsTopology {


    public static String AGGREGATE = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //Streaming from the Kafka topic named AGGREGATE
        var inputStream =
                streamsBuilder
                        .stream(AGGREGATE, Consumed.with(Serdes.String(), Serdes.String()));

        inputStream.print(Printed.<String,String>toSysOut().withLabel(AGGREGATE));

        //group the records for stateful operation
        var groupedStream = inputStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
                        //.groupBy((key, value) -> value, //change of key , aggregate by value
                                //Grouped.with(Serdes.String(), Serdes.String()));

        exploreCount(groupedStream);
        exploreReduce(groupedStream);
        //exploreAggregate(groupedStream);

        return streamsBuilder.build();
    }

    private static void exploreAggregate(KGroupedStream<String, String> groupedStream) {
        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer =
                AlphabetWordAggregate::new;

        Aggregator<String, String, AlphabetWordAggregate> aggregator =
                (((key, value, aggregate) -> aggregate.updateNewEvents(key, value)));

        var aggregatedStream = groupedStream
                .aggregate(alphabetWordAggregateInitializer,
                        aggregator,
                        Materialized.<String, AlphabetWordAggregate, KeyValueStore<Bytes, byte[]>>
                                as("aggregate-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.alphabetWordAggregate())
                );

        aggregatedStream
                .toStream()
                .print(Printed.<String, AlphabetWordAggregate>toSysOut().withLabel("aggregated-words"));
    }

    private static void exploreReduce(KGroupedStream<String, String> groupedStream) {
        var reducedStream = groupedStream
                .reduce((value1, value2) -> {
                    log.info("value1 : {}, value2 : {}", value1, value2);
                    return value1.toUpperCase()+ "-" + value2.toUpperCase();
                },
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduced-words")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );

        reducedStream
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("reduced-words"));
    }

    private static void exploreCount(KGroupedStream<String, String> groupedStream) {
        //KGroupedStream<String, String> changed to KTable<String, Long>
        var countByAlphabet = groupedStream
                .count(Named.as("count-per-alphabet"),
                        Materialized.as("count-per-alphabet"));


        countByAlphabet
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel("words-count-per-alphabet"));
    }

}
