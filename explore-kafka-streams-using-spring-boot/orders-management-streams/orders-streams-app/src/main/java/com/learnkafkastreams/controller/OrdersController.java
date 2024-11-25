package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.service.OrderService;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/orders") // base path
public class OrdersController {

    private OrderService orderService;

    public OrdersController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping("/count/{order_type}")//order_type is path variable
    //return list of java objects(OrderCountPerStoreDTO) ->
    // order count for the store locations based on the order type(whether general or restaurant)
    public ResponseEntity<?> orderCount(
            @PathVariable("order_type") String orderType,
            @RequestParam(value = "location_id", required = false) String locationId) {

        //call this method if location_id is in the URI and return the
        // the orderCountDTO only for that locationId
        if(StringUtils.hasLength(locationId)) {
            return ResponseEntity.ok(orderService.getOrderCountByLocationId(orderType, locationId));
        }

        //return the order count in the form of java object by building a mapper function that
        //converts the data in the state store to order count DTO
        return ResponseEntity.ok(orderService.getOrdersCount(orderType));
    }

    @GetMapping("/count")
    public List<AllOrdersCountPerStoreDTO> getAllOrdersCountPerStore(){
        return orderService.getAllOrdersCount();
    }


    @GetMapping("/revenue/{order_type}")//order_type is path variable
    //return list of java objects(OrderCountPerStoreDTO) ->
    // order count for the store locations based on the order type(whether general or restaurant)
    public ResponseEntity<?> revenueByOrderType(
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
        return ResponseEntity.ok(orderService.revenueByOrderType(orderType));
    }

}
