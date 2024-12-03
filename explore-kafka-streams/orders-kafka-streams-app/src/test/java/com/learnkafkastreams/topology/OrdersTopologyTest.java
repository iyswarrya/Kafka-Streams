package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static com.learnkafkastreams.topology.OrdersTopology.ORDERS;

class OrdersTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, Order> ordersInputTopic = null;

    static String INPUT_TOPIC = ORDERS;

    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(OrdersTopology.buildTopology());

        ordersInputTopic = topologyTestDriver.createInputTopic(ORDERS,
                Serdes.String().serializer(), SerdesFactory.orderSerdes().serializer()
        );

    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }



    @Test
    void ordersCount(){
        //supply the order data to ordersInputTopic
        ordersInputTopic.pipeKeyValueList(orders());

            //read the GENERAL_ORDERS_COUNT state store
        ReadOnlyKeyValueStore<String, Long> generalOrdersCountStore = topologyTestDriver
                .getKeyValueStore(GENERAL_ORDERS_COUNT);

        //get the value for store_1234 from state store
        var generalOrdersCount = generalOrdersCountStore.get("store_1234");
        assertEquals(1, generalOrdersCount); // since one order supplied to the topic for store_1234

        //read the RESTAURANT_ORDERS_COUNT state store
        ReadOnlyKeyValueStore<String, Long> restaurantOrdersCountStore = topologyTestDriver
                .getKeyValueStore(RESTAURANT_ORDERS_COUNT);

        var restaurantOrdersCount = restaurantOrdersCountStore.get("store_1234");
        assertEquals(1, restaurantOrdersCount);

    }

    @Test
    void ordersRevenue(){
        //supply the order data to ordersInputTopic
        ordersInputTopic.pipeKeyValueList(orders());

        ReadOnlyKeyValueStore<String, TotalRevenue> generalOrdersRevenueStore = topologyTestDriver
                .getKeyValueStore(GENERAL_ORDERS_REVENUE);


        var generalOrdersRevenue = generalOrdersRevenueStore.get("store_1234");
        assertEquals(1, generalOrdersRevenue.runningOrderCount()); // since one order supplied to the topic for store_1234
        assertEquals(new BigDecimal("27.00"), generalOrdersRevenue.runningRevenue());


        ReadOnlyKeyValueStore<String, TotalRevenue> restaurantOrdersRevenueStore = topologyTestDriver
                .getKeyValueStore(RESTAURANT_ORDERS_REVENUE);

        var restaurantOrdersRevenue = restaurantOrdersRevenueStore.get("store_1234");
        assertEquals(1, restaurantOrdersRevenue.runningOrderCount());
        assertEquals(new BigDecimal("15.00"), restaurantOrdersRevenue.runningRevenue());
    }

    @Test
    void ordersRevenue_multipleOrdersPerStore(){
        //supply the order data to ordersInputTopic
        ordersInputTopic.pipeKeyValueList(orders());
        ordersInputTopic.pipeKeyValueList(orders());


        ReadOnlyKeyValueStore<String, TotalRevenue> generalOrdersRevenueStore = topologyTestDriver
                .getKeyValueStore(GENERAL_ORDERS_REVENUE);

        //get the value for store_1234 from state store
        var generalOrdersRevenue = generalOrdersRevenueStore.get("store_1234");
        assertEquals(2, generalOrdersRevenue.runningOrderCount()); // since one order supplied to the topic for store_1234
        assertEquals(new BigDecimal("54.00"), generalOrdersRevenue.runningRevenue());


        ReadOnlyKeyValueStore<String, TotalRevenue> restaurantOrdersRevenueStore = topologyTestDriver
                .getKeyValueStore(RESTAURANT_ORDERS_REVENUE);

        var restaurantOrdersRevenue = restaurantOrdersRevenueStore.get("store_1234");
        assertEquals(2, restaurantOrdersRevenue.runningOrderCount());
        assertEquals(new BigDecimal("30.00"), restaurantOrdersRevenue.runningRevenue());
    }

    @Test
    void ordersRevenue_byWindows(){
        //supply multiple order data to ordersInputTopic
        ordersInputTopic.pipeKeyValueList(orders());
        ordersInputTopic.pipeKeyValueList(orders());


        WindowStore<String, TotalRevenue> generalOrdersRevenue = topologyTestDriver
                .getWindowStore(GENERAL_ORDERS_REVENUE_WINDOWS);

        //get the value for store_1234 from state store
        generalOrdersRevenue
                .all()
                .forEachRemaining(windowedTotalRevenueKeyValue ->
                {
                    //keeping the window static for testing
                    var startTime = windowedTotalRevenueKeyValue.key.window().startTime();
                    var endTime = windowedTotalRevenueKeyValue.key.window().endTime();
                    //2024-25-11T12:28:00 start time
                    //2024-25-11T12:28:15 end time

                    var expectedStartTime = LocalDateTime.parse(2024-25-11T11:28:00);
                    var expectedEndTime = LocalDateTime.parse(2024-25-11T11:28:15);

                    assert LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST"))).equals(exceptedStartTime);
                    assert LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST"))).equals(exceptedEndTime);

                    var totalRevenue = windowedTotalRevenueKeyValue.value;
                    assertEquals(2, totalRevenue.runningOrderCount());
                    assertEquals(new BigDecimal("54.00"), generalOrdersRevenue.runningRevenue());

                })

        WindowStore<String, TotalRevenue> restaurantOrdersRevenue = topologyTestDriver
                .getWindowStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);

        restaurantOrdersRevenue
                .all()
                .forEachRemaining(windowedTotalRevenueKeyValue ->
                {
                    //keeping the window static for testing
                    var startTime = windowedTotalRevenueKeyValue.key.window().startTime();
                    var endTime = windowedTotalRevenueKeyValue.key.window().endTime();
                    //2024-25-11T12:28:00 start time
                    //2024-25-11T12:28:15 end time

                    var expectedStartTime = LocalDateTime.parse(2024-25-11T11:28:00);
                    var expectedEndTime = LocalDateTime.parse(2024-25-11T11:28:15);

                    //validating windows
                    assert LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST"))).equals(exceptedStartTime);
                    assert LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST"))).equals(exceptedEndTime);
                    var totalRevenue = windowedTotalRevenueKeyValue.value;
                    assertEquals(2, totalRevenue.runningOrderCount());
                    assertEquals(new BigDecimal("54.00"), generalOrdersRevenue.runningRevenue());

                })
    }





    static List<KeyValue<String, Order>> orders(){

        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.parse(2024-25-11T11:28:01)
                //LocalDateTime.now()
                //LocalDateTime.now(ZoneId.of("UTC"))
        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.parse(2024-25-11T11:28:01)
                //LocalDateTime.now()
                //LocalDateTime.now(ZoneId.of("UTC"))
        );
        var keyValue1 = KeyValue.pair( order1.orderId().toString()
                , order1);

        var keyValue2 = KeyValue.pair( order2.orderId().toString()
                , order2);


        return  List.of(keyValue1, keyValue2);

    }
}