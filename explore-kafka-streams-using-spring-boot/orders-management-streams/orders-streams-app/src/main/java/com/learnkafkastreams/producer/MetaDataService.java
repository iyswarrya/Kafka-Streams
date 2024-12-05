package com.learnkafkastreams.producer;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.domain.HostInfoDTOWithKey;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class MetaDataService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public MetaDataService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public List<HostInfoDTO> getStreamsMetaData() {
        return streamsBuilderFactoryBean
                // Get the KafkaStreams instances from the factory bean
                .getKafkaStreams()
                // Retrieve metadata for all Kafka Streams clients
                .metadataForAllStreamsClients()
                .stream()
                // Map each StreamsMetadata to a HostInfoDTO
                .map(streamsMetadata -> {
                    var hostInfo = streamsMetadata.hostInfo();
                    return new HostInfoDTO(hostInfo.host(), hostInfo.port());
                })
                // Collect the transformed data into a List
                .collect(Collectors.toList());

    }

    public HostInfoDTOWithKey getStreamsMetaData(String storeName, String locationId) {
        var metaDataForKey =  streamsBuilderFactoryBean
                // Get the KafkaStreams instances from the factory bean
                .getKafkaStreams()
                // Retrieve metadata for all Kafka Streams clients
                .queryMetadataForKey(storeName, locationId, Serdes.String().serializer());

        if (metaDataForKey != null) {
            var activeHost = metaDataForKey.activeHost();
            return new HostInfoDTOWithKey(activeHost.host(), activeHost.port(), locationId);
        }
        return null;

    }


}
