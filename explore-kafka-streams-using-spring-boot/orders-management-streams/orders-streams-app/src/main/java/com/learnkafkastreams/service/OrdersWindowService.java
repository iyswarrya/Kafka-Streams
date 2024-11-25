package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.service.OrderService.mapOrderType;
import static com.learnkafkastreams.topology.OrdersTopology.*;

@Slf4j
@Service
public class OrdersWindowService {

    private final OrderStoreService orderStoreService;

    public OrdersWindowService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    //gets the orderType and creates a json like the one in 05-orders-count-windows.json
    //this json holds the order count aggregated between start and end time for every order type
    public List<OrdersCountPerStoreByWindowsDTO> getOrdersCountWindowsByType(String orderType) {
        var countWindowsStore = getCountWindowsStore(orderType);
        var orderTypeEnum = mapOrderType(orderType);

        var countWindows = countWindowsStore.all();//gives an iterator for iterating through all the keys returned

        //iterate the records one by one and map it to a list of OrderCountPerStoreDTO
        return mapToOrdersCountPerStoreByWindowsDTO(countWindows, orderTypeEnum);

    }

    private static List<OrdersCountPerStoreByWindowsDTO> mapToOrdersCountPerStoreByWindowsDTO(KeyValueIterator<Windowed<String>, Long> countWindows, OrderType orderTypeEnum) {
        var spliterator = Spliterators.spliteratorUnknownSize(countWindows, 0);

        //create a new stream from spliterator
        return StreamSupport.stream(spliterator, false)
                //convert from keyValue type to OrderCountPerStoreDTO
                .map(keyValue -> new OrdersCountPerStoreByWindowsDTO(
                                keyValue.key.key() // since windowed string
                                , keyValue.value
                                , orderTypeEnum
                                //change from Instant type to LocalDateTime type
                                , LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT"))
                                , LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                        )
                )
                .collect(Collectors.toList());
    }

    private static List<OrdersRevenuePerStoreByWindowsDTO> mapToOrdersRevenuePerStoreByWindowsDTO(KeyValueIterator<Windowed<String>, TotalRevenue> revenueWindows, OrderType orderTypeEnum) {
        var spliterator = Spliterators.spliteratorUnknownSize(revenueWindows, 0);

        //create a new stream from spliterator
        return StreamSupport.stream(spliterator, false)
                //convert from keyValue type to OrderCountPerStoreDTO
                .map(keyValue -> new OrdersRevenuePerStoreByWindowsDTO(
                                keyValue.key.key() // since windowed string
                                , keyValue.value
                                , orderTypeEnum
                                //change from Instant type to LocalDateTime type
                                , LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT"))
                                , LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                        )
                )
                .collect(Collectors.toList());
    }

    private ReadOnlyWindowStore<String, Long> getCountWindowsStore(String orderType) {
        return switch (orderType){

            case GENERAL_ORDERS -> orderStoreService.ordersWindowsCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS);
            default -> throw new IllegalStateException("Unexpected value: " + orderType);
        };
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows() {

        var generalOrdersWindowsCount = getOrdersCountWindowsByType(GENERAL_ORDERS);

        //get all the restaurantOrdersCount by converting the OrderCountPerStoreDTO to stream
        //then transform the list to list of AllOrdersCountPerStoreDTO using the mapper defined above
        var restaurantOrdersWindowsCount = getOrdersCountWindowsByType(RESTAURANT_ORDERS);

        //flatMap is used here to flatten the [[general orders List][restaurant orders list]]
        //into a single list [All orders list]
        //first convert the general orders List and restaurant orders list to stream to apply flatMap
        return Stream.of(generalOrdersWindowsCount, restaurantOrdersWindowsCount)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows
            (LocalDateTime fromTime, LocalDateTime toTime) {

        var fromTimeInstant = fromTime.toInstant(ZoneOffset.UTC);
        var toTimeInstant = toTime.toInstant(ZoneOffset.UTC);

        var generalOrdersWindowsCount = getCountWindowsStore(GENERAL_ORDERS)
                .backwardFetchAll(fromTimeInstant, toTimeInstant);

        var restaurantOrdersWindowsCount = getCountWindowsStore(RESTAURANT_ORDERS)
                .backwardFetchAll(fromTimeInstant, toTimeInstant);

        var generalOrdersWindowsCountDTO
                = mapToOrdersCountPerStoreByWindowsDTO(generalOrdersWindowsCount, mapOrderType(GENERAL_ORDERS));

        var restaurantOrdersWindowsCountDTO
                = mapToOrdersCountPerStoreByWindowsDTO(restaurantOrdersWindowsCount, mapOrderType(RESTAURANT_ORDERS));

        return Stream.of(generalOrdersWindowsCountDTO, restaurantOrdersWindowsCountDTO)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }

    public List<OrdersRevenuePerStoreByWindowsDTO> revenueByOrderTypeByWindows(String orderType) {
        var revenueStoreByWindows = getRevenueStoreByWindows(orderType);
        var orderTypeEnum = mapOrderType(orderType);
        var revenueByOrdersByWindows = revenueStoreByWindows.all();
        return mapToOrdersRevenuePerStoreByWindowsDTO(revenueByOrdersByWindows, orderTypeEnum);

    }

    private ReadOnlyWindowStore<String, TotalRevenue> getRevenueStoreByWindows(String orderType) {
        return switch (orderType){
            case GENERAL_ORDERS -> orderStoreService.ordersWindowsRevenueStore(GENERAL_ORDERS_REVENUE_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsRevenueStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
            default -> throw new IllegalStateException("Unexpected value: " + orderType);
        };
    }
}
