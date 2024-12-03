package com.learnkafkastreams.service;

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

    @Value("${server.port}")
    private Integer port;

    public OrderService(OrderStoreService orderStoreService, MetaDataService metaDataService) {
        this.orderStoreService = orderStoreService;
        this.metaDataService = metaDataService;
    }

    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) throws UnknownHostException {
        //access the state store and get the order count for each store based on order type
        var ordersCountStore = getOrderStore(orderType);
        var orders = ordersCountStore.all();//gives an iterator for iterating through all the keys returned

        //iterate the records one by one and map it to a list of OrderCountPerStoreDTO
        var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);

        //fetch the metadata about other instances
        //make rest call to get the data from other instance
            //make sure the other instance is not going to make any network call to other instances-other goes in loops
        //aggregate the data
        retrieveDataFromOtherInstances(orderType);


        //create a new stream from spliterator
        return StreamSupport.stream(spliterator, false)
                //convert from keyValue type to OrderCountPerStoreDTO
                .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

    }

    private void retrieveDataFromOtherInstances(String orderType) throws UnknownHostException {
        var otherHosts = otherHosts();
        log.info("otherHosts: {}", otherHosts);

        if(otherHosts != null && !otherHosts.isEmpty()) {

        }
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
        var ordersCountStore = getOrderStore(orderType);
        var orderCount = ordersCountStore.get(locationId);
        if (orderCount !=null){
            return new OrderCountPerStoreDTO(locationId, orderCount);
        }
        return null;
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {
        //this BiFunction takes in OrderCountPerStoreDTO, OrderType as argument type and
        // transform to AllOrdersCountPerStoreDTO
        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO>
                mapper = (orderCountperStoreDTO, orderType) -> new AllOrdersCountPerStoreDTO(
                        orderCountperStoreDTO.locationId(), orderCountperStoreDTO.orderCount(),
                        orderType);

        //get all the generalOrdersCount by converting the OrderCountPerStoreDTO to stream
        //then transform the list to list of AllOrdersCountPerStoreDTO using the mapper defined above
        var generalOrdersCount = getOrdersCount(GENERAL_ORDERS)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
                .toList();

        //get all the restaurantOrdersCount by converting the OrderCountPerStoreDTO to stream
        //then transform the list to list of AllOrdersCountPerStoreDTO using the mapper defined above
        var restaurantOrdersCount = getOrdersCount(RESTAURANT_ORDERS)
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
