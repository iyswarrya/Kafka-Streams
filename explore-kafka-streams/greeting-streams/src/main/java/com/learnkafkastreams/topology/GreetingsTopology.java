package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.kstream.EmitStrategy.log;

public class GreetingsTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static String GREETINGS_SPANISH = "greetings_spanish";

    StreamsBuilder streamsBuilder1 = new StreamsBuilder();

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //KStream<String, String> mergedStream = getStringGreetingKStream(streamsBuilder);
        KStream<String, Greeting> mergedStream = getCustomGreetingKStream(streamsBuilder);
        mergedStream.print(Printed.<String, Greeting>toSysOut().withLabel("mergedStream"));

        //var modifiedStream = exploreOperators(mergedStream);
        var modifiedStream = exploreErrors(mergedStream);

        modifiedStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedStream"));

        modifiedStream.to(GREETINGS_UPPERCASE
                //, Produced.with(Serdes.String(), Serdes.String())
                , Produced.with(Serdes.String(), SerdesFactory.greetingSerdeGenerics())
                 );
        return streamsBuilder.build();
    }

    private static KStream<String, Greeting> exploreErrors(KStream<String, Greeting> mergedStream) {
        return mergedStream
                .mapValues((readOnlyKey, value) -> {
                    if (value.getMessage().equals("Transient Error")) {
                        try{
                            throw new IllegalStateException(value.getMessage());
                        }catch (Exception e){
                            log.error("Exception in exploreErrors : {}", e.getMessage(), e);
                            return null;
                        }

                    }
                    return new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp());
                }
                )
                .filter((key,value) -> key != null && value != null);
    }

    private static KStream<String, Greeting> exploreOperators(KStream<String, Greeting> mergedStream) {
        var modifiedStream = mergedStream
                //.filter((key, value) -> value.length() > 5)
                //.peek((key, value) -> {
                //    log.info("after filter: key : {}, value : {}", key, value);
                //})
                .mapValues((readOnlyKey, value)-> new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp()));
                /*.peek((key, value) -> {
                    log.info("after mapValues: key : {}, value : {}", key, value);
                });;*/
        //.map((key, value) -> KeyValue.pair(key.toUpperCase(),value.toUpperCase()));
                /*.flatMapValues((value)-> {
                            var newValues = Arrays.asList(value.split(""));
                            return newValues
                                    .stream()
                                    .map(String::toUpperCase)
                                    .collect(Collectors.toList());
                        });*/
        return modifiedStream;
    }

    private static KStream<String, String> getStringGreetingKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> greetingsStream = streamsBuilder.stream(GREETINGS//
                // , Consumed.with(Serdes.String(), Serdes.String())
                );
        //greetingsStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));

        KStream<String, String> greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH
                //, Consumed.with(Serdes.String(), Serdes.String())
                );

        return greetingsStream.merge(greetingsSpanishStream);
    }

    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {
        var greetingsStream = streamsBuilder.stream(GREETINGS
               , Consumed.with(Serdes.String(), SerdesFactory.greetingSerdeGenerics())
        );
        //greetingsStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));

        var greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH
                , Consumed.with(Serdes.String(), SerdesFactory.greetingSerdeGenerics())
        );

        return greetingsStream.merge(greetingsSpanishStream);
    }
}
