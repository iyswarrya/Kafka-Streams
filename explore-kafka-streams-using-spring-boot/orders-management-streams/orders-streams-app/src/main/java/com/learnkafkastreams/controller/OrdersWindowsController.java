package com.learnkafkastreams.controller;


import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.service.OrderService;
import com.learnkafkastreams.service.OrdersWindowService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/v1/orders") //
public class OrdersWindowsController {

    private final OrdersWindowService ordersWindowService;

    public OrdersWindowsController(OrdersWindowService ordersWindowService) {
        this.ordersWindowService = ordersWindowService;
    }

    @GetMapping("/windows/count/{order_type}")
    public List<OrdersCountPerStoreByWindowsDTO> ordersCountWindowsByType(@PathVariable("order_type") String orderType){
        return ordersWindowService.getOrdersCountWindowsByType(orderType);
    }

    @GetMapping("/windows/count")
    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountPerStoreByWindows
            (@RequestParam(value = "from_time", required = false)
             @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime fromTime,
             @RequestParam(value = "to_time", required = false)
             @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime toTime){

        if(fromTime != null && toTime != null){
            return ordersWindowService.getAllOrdersCountByWindows(fromTime, toTime);
        }
        return ordersWindowService.getAllOrdersCountByWindows();
    }


    @GetMapping("/windows/revenue/{order_type}")//order_type is path variable
    public List<OrdersRevenuePerStoreByWindowsDTO> revenueByOrderTypeByWindows(
            @PathVariable("order_type") String orderType
            //@RequestParam(value = "location_id", required = false) String locationId
    ) {

        //call this method if location_id is in the URI and return the
        // the orderCountDTO only for that locationId
//        if(StringUtils.hasLength(locationId)) {
//            return ResponseEntity.ok(orderService.getOrderCountByLocationId(orderType, locationId));
//        }

        //return the order count in the form of java object by building a mapper function that
        //converts the data in the state store to order count DTO
        return ordersWindowService.revenueByOrderTypeByWindows(orderType);
    }



}
