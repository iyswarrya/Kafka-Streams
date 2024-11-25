package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Alphabet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

@Slf4j
public class ExploreJoinsOperatorsTopology {


    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple


    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //joinKStreamwithKTable(streamsBuilder);
        //joinKStreamWithGlobalKTable(streamsBuilder);
        //joinKTablewithKTable(streamsBuilder);
        joinKStreamwithKStream(streamsBuilder);

        return streamsBuilder.build();
    }

    private static void joinKStreamwithKTable(StreamsBuilder streamsBuilder) {
        var alphabetsAbbrevation = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbrevation.print(Printed.<String, String>toSysOut().withLabel("alphabets_abbreviations"));

        var alphabetsTable = streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String())
                , Materialized.as("alphabets-store"));

        alphabetsTable.toStream().print(Printed.<String, String>toSysOut().withLabel("alphabets"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var joinedStream = alphabetsAbbrevation.join(alphabetsTable, valueJoiner);

        joinedStream.print(Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbrevation"));
    }

    private static void joinKStreamwithKStream(StreamsBuilder streamsBuilder) {
        var alphabetsAbbrevation = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbrevation.print(Printed.<String, String>toSysOut().withLabel("alphabets_abbreviations"));

        var alphabetsStream = streamsBuilder
                .stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsStream.print(Printed.<String, String>toSysOut().withLabel("alphabets"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var fiveSecondWindow = JoinWindows
                .ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        var joinedParams = StreamJoined
                .with(Serdes.String(), Serdes.String(), Serdes.String())
                .withName("alphabets-join")
                .withStoreName("alphabets-join");

        var joinedStream = alphabetsAbbrevation
                .outerJoin(alphabetsStream, valueJoiner, fiveSecondWindow, joinedParams);

        joinedStream.print(Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbrevation-kstream"));
    }

    private static void joinKTablewithKTable(StreamsBuilder streamsBuilder) {
        var alphabetsAbbrevation = streamsBuilder
                .table(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String())
                , Materialized.as("alphabets-abbrevation-store"));

        alphabetsAbbrevation.
                toStream()
                .print(Printed.<String, String>toSysOut().withLabel("alphabets_abbreviations"));

        var alphabetsTable = streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as("alphabets-store"));

        alphabetsTable.toStream().print(Printed.<String, String>toSysOut().withLabel("alphabets"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var joinedStream = alphabetsAbbrevation.join(alphabetsTable, valueJoiner);

        joinedStream.
                toStream()
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbrevation"));
    }

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {
        var alphabetsAbbrevation = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbrevation.print(Printed.<String, String>toSysOut().withLabel("alphabets_abbreviations"));

        var alphabetsTable = streamsBuilder
                .globalTable(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as("alphabets-store"));

        //alphabetsTable.toStream().print(Printed.<String, String>toSysOut().withLabel("alphabets"));

        KeyValueMapper<String, String, String> keyValueMapper =
                (leftKey, rightKey) -> leftKey;

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var joinedStream = alphabetsAbbrevation.join(alphabetsTable, keyValueMapper, valueJoiner);

        joinedStream.print(Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbrevation"));
    }

}
