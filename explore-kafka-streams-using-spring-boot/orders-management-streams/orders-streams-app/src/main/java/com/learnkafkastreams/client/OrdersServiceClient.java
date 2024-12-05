package com.learnkafkastreams.client;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;

@Component
@Slf4j
public class OrdersServiceClient {

    private WebClient webClient;

    public OrdersServiceClient(WebClient webClient) {
        this.webClient = webClient;
    }

    public List<OrderCountPerStoreDTO> retrieveOrderCountByOrderType(HostInfoDTO hostInfoDTO,
                                                                     String orderType) {
        //curl -i http://localhost:8080/v1/orders/count/general_orders
        var basePath = "http://"+hostInfoDTO.host()+":"+hostInfoDTO.port();

        //build the url
        var url = UriComponentsBuilder
                .fromHttpUrl(basePath)
                .path("/v1/orders/count/{order_type}")
                .queryParam("query_other_hosts", "false")
                .buildAndExpand(orderType)
                .toString();
        log.info("retrieveOrdersCountByOrderType: {}", url);

        //Send a GET request to the constructed URL using a webClient instance
        return webClient.get()
                .uri(url)
                .retrieve()
                //used to deserialize an HTTP response body
                //into a reactive stream of objects of the type OrderCountPerStoreDTO
                .bodyToFlux(OrderCountPerStoreDTO.class)
                .collectList()
                //It waits for the entire stream to complete and blocks the thread.
                //If the stream emits multiple items, .block() throws an IllegalStateException.
                //To work with all emitted items, you should first collect them into a List (e.g., using .collectList()).
                .block();
    }


    public OrderCountPerStoreDTO retrieveOrderCountByOrderTypeAndLocation(HostInfoDTO hostInfoDTO, String orderType, String locationId) {
        var basePath = "http://"+hostInfoDTO.host()+":"+hostInfoDTO.port();

        //build the url
        var url = UriComponentsBuilder
                .fromHttpUrl(basePath)
                .path("/v1/orders/count/{order_type}")
                .queryParam("query_other_hosts", "false")
                .queryParam("location_id", locationId)
                .buildAndExpand(orderType)
                .toString();
        log.info("retrieveOrdersCountByOrderTypeAndLocationId: {}", url);

        return webClient.get()
                .uri(url)
                .retrieve()
                //used to deserialize an HTTP response body
                .bodyToMono(OrderCountPerStoreDTO.class)
                //It waits for the entire stream to complete and blocks the thread.
                //If the stream emits multiple items, .block() throws an IllegalStateException.
                //To work with all emitted items, you should first collect them into a List (e.g., using .collectList()).
                .block();


    }
}
