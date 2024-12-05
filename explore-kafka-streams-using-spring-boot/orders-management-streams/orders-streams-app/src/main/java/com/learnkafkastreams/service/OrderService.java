package com.learnkafkastreams.service;

import com.learnkafkastreams.client.OrdersServiceClient;
import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.producer.MetaDataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.topology.OrdersTopology.*;


@Service
@Slf4j
public class OrderService {

    private OrderStoreService orderStoreService;
    private MetaDataService metaDataService;
    private OrdersServiceClient ordersServiceClient;

    @Value("${server.port}")
    private Integer port;



    public OrderService(OrderStoreService orderStoreService,
                        MetaDataService metaDataService, OrdersServiceClient ordersServiceClient) {
        this.orderStoreService = orderStoreService;
        this.metaDataService = metaDataService;
        this.ordersServiceClient = ordersServiceClient;
    }

    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType, String queryOtherHosts) throws UnknownHostException {
        //access the state store and get the order count for each store based on order type
        var ordersCountStore = getOrderStore(orderType);
        var orders = ordersCountStore.all();//gives an iterator for iterating through all the keys returned

        //iterate the records one by one and map it to a list of OrderCountPerStoreDTO
        var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);
        var orderCounterPerStoreDTOListCurrentInstance = StreamSupport.stream(spliterator, false)
                //convert from keyValue type to OrderCountPerStoreDTO
                .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

        //fetch the metadata about other instances
        //make rest call to get the data from other instance
            //make sure the other instance is not going to make any network call to other instances-other goes in loops
        var orderCounterPerStoreDTOList = retrieveDataFromOtherInstances(orderType,
                Boolean.parseBoolean(queryOtherHosts));

        log.info("orderCounterPerStoreDTOList : {}, orderCounterPerStoreDTOList : {}",
                orderCounterPerStoreDTOListCurrentInstance,
                orderCounterPerStoreDTOList);
        //aggregate the data
        return Stream.of(orderCounterPerStoreDTOListCurrentInstance, orderCounterPerStoreDTOList)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }

    private List<OrderCountPerStoreDTO> retrieveDataFromOtherInstances(String orderType, boolean queryOtherHosts) throws UnknownHostException {
        var otherHosts = otherHosts();
        log.info("otherHosts: {}", otherHosts);

        if(queryOtherHosts && otherHosts != null && !otherHosts.isEmpty()) {
            return otherHosts()
                    .stream()
                    .map(hostInfoDTO -> ordersServiceClient
                            .retrieveOrderCountByOrderType(hostInfoDTO, orderType))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

        }
        return null;
    }

    private List<HostInfoDTO> otherHosts() throws java.net.UnknownHostException {
        try {
            var currentMachineAddress = InetAddress.getLocalHost().getHostAddress();
            return metaDataService.getStreamsMetaData()
                    .stream()
                    .filter(hostInfoDTO -> hostInfoDTO.port() != port)
                    .collect(Collectors.toList());
        }
        catch (UnknownHostException e) {
            log.error("Exception in other hosts: {}", e.getMessage(), e);
        }
        return null;
    }

    public ReadOnlyKeyValueStore<String, Long> getOrderStore(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> orderStoreService.ordersCountStore(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> orderStoreService.ordersCountStore(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Unexpected value: " + orderType);
        };
    }

    public OrderCountPerStoreDTO getOrderCountByLocationId(String orderType, String locationId) {
        var storeName = mapOrderCountStoreName(orderType);
        var hostMetaData = metaDataService.getStreamsMetaData(storeName, locationId);
        log.info("hostMetaData: {}", hostMetaData);
        if (hostMetaData != null) {
            if(hostMetaData.port() != port) {
                log.info("Fetching the data from the current instance");
                var ordersCountStore = getOrderStore(orderType);
                var orderCount = ordersCountStore.get(locationId);
                if (orderCount !=null){
                    return new OrderCountPerStoreDTO(locationId, orderCount);
                }
            }return null;
        }else{
            log.info("Fetching the data from the remote instance");
            var orderCountPerStoreDTO =  ordersServiceClient.retrieveOrderCountByOrderTypeAndLocation(
                    new HostInfoDTO(hostMetaData.host(), hostMetaData.port()),
                    orderType, locationId
            );
            return orderCountPerStoreDTO;
        }

    }

    private String mapOrderCountStoreName(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> GENERAL_ORDERS_COUNT;
            case RESTAURANT_ORDERS -> RESTAURANT_ORDERS_COUNT;
            default -> throw new IllegalStateException("Unexpected value: " + orderType);
        };
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() throws UnknownHostException {
        //this BiFunction takes in OrderCountPerStoreDTO, OrderType as argument type and
        // transform to AllOrdersCountPerStoreDTO
        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO>
                mapper = (orderCountperStoreDTO, orderType) -> new AllOrdersCountPerStoreDTO(
                        orderCountperStoreDTO.locationId(), orderCountperStoreDTO.orderCount(),
                        orderType);

        //get all the generalOrdersCount by converting the OrderCountPerStoreDTO to stream
        //then transform the list to list of AllOrdersCountPerStoreDTO using the mapper defined above
        var generalOrdersCount = getOrdersCount(GENERAL_ORDERS, "true")
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
                .toList();

        //get all the restaurantOrdersCount by converting the OrderCountPerStoreDTO to stream
        //then transform the list to list of AllOrdersCountPerStoreDTO using the mapper defined above
        var restaurantOrdersCount = getOrdersCount(RESTAURANT_ORDERS, "true")
                .stream()
                .map((orderCountPerStoreDTO) -> mapper.apply(orderCountPerStoreDTO, OrderType.RESTAURANT))
                .toList();

        //flatMap is used here to flatten the [[general orders List][restaurant orders list]]
        //into a single list [All orders list]
        //first convert the general orders List and restaurant orders list to stream to apply flatMap
        return Stream.of(generalOrdersCount, restaurantOrdersCount)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }


    public List<OrderRevenueDTO> revenueByOrderType(String orderType) {

        var revenueStoreByType = getRevenueStore(orderType);
        var revenueByOrders = revenueStoreByType.all();//gives an iterator for iterating through all the keys returned

        //iterate the records one by one and map it to a list of OrderCountPerStoreDTO
        var spliterator = Spliterators.spliteratorUnknownSize(revenueByOrders, 0);

        //create a new stream from spliterator
        return StreamSupport.stream(spliterator, false)
                //convert from keyValue type to OrderRevenueDTO
                .map(keyValue -> new OrderRevenueDTO(keyValue.key, mapOrderType(orderType), keyValue.value))
                .collect(Collectors.toList());

    }

    public static OrderType mapOrderType(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Not a valid option");
        };
    }

    //return a state store with location ID as the key and the revenue at the location
    private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> orderStoreService.ordersRevenueStore(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> orderStoreService.ordersRevenueStore(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Unexpected value: " + orderType);
        };
    }
}
