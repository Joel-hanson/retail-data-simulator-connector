package com.retaildatasimulator.kafka.connect.source;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetailDataSimulatorTask extends SourceTask {
        private static final Logger log = LoggerFactory.getLogger(RetailDataSimulatorTask.class);
        private static final String POSITION_FIELD = "position";

        private RetailDataSimulatorConfig config;
        private String inventoryTopic;
        private String ordersTopic;
        private String logisticsTopic;
        private long pollIntervalMs;
        private int batchSize;

        private AtomicLong currentPosition = new AtomicLong();
        private Map<String, Object> offset;

        // Schemas
        private Schema inventorySchema;
        private Schema orderSchema;
        private Schema orderItemSchema;
        private Schema logisticsSchema;
        private Schema costComponentSchema;

        // Faker-like data generators
        private final List<String> shoeModels = Arrays.asList("270", "90", "97");
        private final List<String> suppliers = Arrays.asList("Nike Direct", "Foot Locker", "Amazon");
        private final List<String> carriers = Arrays.asList("FedEx", "UPS", "USPS");
        private final List<String> statuses = Arrays.asList("in_transit", "delayed", "out_for_delivery");
        private final List<String> paymentMethods = Arrays.asList("credit_card", "paypal", "store_credit");
        private final List<String> cities = Arrays.asList("New York", "Los Angeles", "Chicago", "Houston", "Phoenix");

        @Override
        public String version() {
                return "0.0.1";
        }

        @Override
        public void start(Map<String, String> props) {
                log.info("Starting Retail Data Simulator Task");
                config = new RetailDataSimulatorConfig(props);

                inventoryTopic = config.getInventoryTopic();
                ordersTopic = config.getOrdersTopic();
                logisticsTopic = config.getLogisticsTopic();
                pollIntervalMs = config.getPollIntervalMs();
                batchSize = config.getBatchSize();

                initializeSchemas();

                offset = context.offsetStorageReader().offset(sourcePartition());
                if (offset != null && offset.containsKey(POSITION_FIELD)) {
                        currentPosition.set((Long) offset.get(POSITION_FIELD));
                        log.info("Loaded offset: {}", currentPosition.get());
                } else {
                        currentPosition.set(0L);
                        log.info("No stored offset, starting from 0");
                }
        }

        private void initializeSchemas() {
                // Order Item Schema
                orderItemSchema = SchemaBuilder.struct()
                                .name("OrderItem")
                                .field("sku", Schema.STRING_SCHEMA)
                                .field("quantity", Schema.INT32_SCHEMA)
                                .field("price", Schema.FLOAT32_SCHEMA)
                                .field("size", Schema.STRING_SCHEMA)
                                .build();

                // Order Schema
                orderSchema = SchemaBuilder.struct()
                                .name("OrderEvent")
                                .field("event_id", Schema.STRING_SCHEMA)
                                .field("event_type", Schema.STRING_SCHEMA)
                                .field("order_id", Schema.STRING_SCHEMA)
                                .field("customer_id", Schema.STRING_SCHEMA)
                                .field("items", SchemaBuilder.array(orderItemSchema))
                                .field("total_amount", Schema.FLOAT32_SCHEMA)
                                .field("payment_method", Schema.STRING_SCHEMA)
                                .field("payment_details", Schema.STRING_SCHEMA)
                                .field("loyalty_points_used", Schema.INT32_SCHEMA)
                                .field("timestamp", Schema.STRING_SCHEMA)
                                .build();

                // Inventory Schema
                inventorySchema = SchemaBuilder.struct()
                                .name("InventoryEvent")
                                .field("event_id", Schema.STRING_SCHEMA)
                                .field("event_type", Schema.STRING_SCHEMA)
                                .field("location", Schema.STRING_SCHEMA)
                                .field("sku", Schema.STRING_SCHEMA)
                                .field("quantity", Schema.INT32_SCHEMA)
                                .field("timestamp", Schema.STRING_SCHEMA)
                                .field("reorder_threshold", Schema.INT32_SCHEMA)
                                .field("supplier", Schema.STRING_SCHEMA)
                                .build();

                // Cost Component Schema
                costComponentSchema = SchemaBuilder.struct()
                                .name("CostComponent")
                                .field("name", Schema.STRING_SCHEMA)
                                .field("value", Schema.FLOAT32_SCHEMA)
                                .build();

                // Logistics Schema
                logisticsSchema = SchemaBuilder.struct()
                                .name("LogisticsEvent")
                                .field("event_id", Schema.STRING_SCHEMA)
                                .field("event_type", Schema.STRING_SCHEMA)
                                .field("shipment_id", Schema.STRING_SCHEMA)
                                .field("carrier", Schema.STRING_SCHEMA)
                                .field("status", Schema.STRING_SCHEMA)
                                .field("estimated_delivery", Schema.STRING_SCHEMA)
                                .field("cost_components", SchemaBuilder.array(costComponentSchema))
                                .field("route_optimization_score", Schema.FLOAT32_SCHEMA)
                                .field("carbon_footprint", Schema.FLOAT32_SCHEMA)
                                .field("timestamp", Schema.STRING_SCHEMA)
                                .build();
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
                if (pollIntervalMs > 0) {
                        Thread.sleep(pollIntervalMs);
                }

                List<SourceRecord> records = new ArrayList<>(batchSize * 3); // For all three event types

                try {
                        for (int i = 0; i < batchSize; i++) {
                                // Generate inventory event
                                Struct inventoryEvent = generateInventoryEvent();
                                records.add(new SourceRecord(
                                                sourcePartition(),
                                                sourceOffset(),
                                                inventoryTopic,
                                                null,
                                                inventorySchema,
                                                inventoryEvent));

                                // Generate order event
                                Struct orderEvent = generateOrderEvent();
                                records.add(new SourceRecord(
                                                sourcePartition(),
                                                sourceOffset(),
                                                ordersTopic,
                                                null,
                                                orderSchema,
                                                orderEvent));

                                // Generate logistics event
                                Struct logisticsEvent = generateLogisticsEvent();
                                records.add(new SourceRecord(
                                                sourcePartition(),
                                                sourceOffset(),
                                                logisticsTopic,
                                                null,
                                                logisticsSchema,
                                                logisticsEvent));

                                currentPosition.incrementAndGet();
                        }
                } catch (Exception e) {
                        log.error("Error generating data", e);
                }

                if (!records.isEmpty()) {
                        log.debug("Generated {} records ({} batches)", records.size(), batchSize);
                }

                return records;
        }

        private Struct generateInventoryEvent() {
                String sku = String.format("Nike-AM-%s-%d",
                                shoeModels.get(ThreadLocalRandom.current().nextInt(shoeModels.size())),
                                ThreadLocalRandom.current().nextInt(8, 13));

                return new Struct(inventorySchema)
                                .put("event_id", UUID.randomUUID().toString())
                                .put("event_type", "inventory_update")
                                .put("location", cities.get(ThreadLocalRandom.current().nextInt(cities.size())))
                                .put("sku", sku)
                                .put("quantity", ThreadLocalRandom.current().nextInt(0, 101))
                                .put("timestamp", Instant.now().toString())
                                .put("reorder_threshold", ThreadLocalRandom.current().nextInt(10, 31))
                                .put("supplier", suppliers.get(ThreadLocalRandom.current().nextInt(suppliers.size())));
        }

        private Struct generateOrderEvent() {
                int itemCount = ThreadLocalRandom.current().nextInt(1, 4);
                List<Struct> items = IntStream.range(0, itemCount)
                                .mapToObj(i -> {
                                        String sku = String.format("Nike-AM-%s-%d",
                                                        shoeModels.get(ThreadLocalRandom.current()
                                                                        .nextInt(shoeModels.size())),
                                                        ThreadLocalRandom.current().nextInt(8, 13));

                                        return new Struct(orderItemSchema)
                                                        .put("sku", sku)
                                                        .put("quantity", ThreadLocalRandom.current().nextInt(1, 3))
                                                        .put("price", round(
                                                                        ThreadLocalRandom.current().nextFloat() * 80f
                                                                                        + 120f,
                                                                        2))
                                                        .put("size", String.valueOf(
                                                                        ThreadLocalRandom.current().nextInt(8, 13)));
                                })
                                .collect(Collectors.toList());

                String paymentMethod = paymentMethods.get(ThreadLocalRandom.current().nextInt(paymentMethods.size()));
                String paymentDetails = paymentMethod.equals("credit_card") ? "Credit Card Number 1234 1234 1234 1234"
                                : "payment done";

                float totalAmount = (float) items.stream()
                                .mapToDouble(item -> item.getInt32("quantity") * item.getFloat32("price"))
                                .sum();

                return new Struct(orderSchema)
                                .put("event_id", UUID.randomUUID().toString())
                                .put("event_type", "order_created")
                                .put("order_id", "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase())
                                .put("customer_id", UUID.randomUUID().toString())
                                .put("items", items)
                                .put("total_amount", round(totalAmount, 2))
                                .put("payment_method", paymentMethod)
                                .put("payment_details", paymentDetails)
                                .put("loyalty_points_used", ThreadLocalRandom.current().nextInt(0, 1001))
                                .put("timestamp", Instant.now().toString());
        }

        private Struct generateLogisticsEvent() {
                List<Struct> costComponents = Arrays.asList(
                                new Struct(costComponentSchema)
                                                .put("name", "base")
                                                .put("value", round(ThreadLocalRandom.current().nextFloat() * 15f + 5f,
                                                                2)),
                                new Struct(costComponentSchema)
                                                .put("name", "fuel_surcharge")
                                                .put("value", round(ThreadLocalRandom.current().nextFloat() * 4f + 1f,
                                                                2)),
                                new Struct(costComponentSchema)
                                                .put("name", "priority_fee")
                                                .put("value", round(ThreadLocalRandom.current().nextFloat() * 10f, 2)));

                return new Struct(logisticsSchema)
                                .put("event_id", UUID.randomUUID().toString())
                                .put("event_type", "shipment_update")
                                .put("shipment_id",
                                                "SHIP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase())
                                .put("carrier", carriers.get(ThreadLocalRandom.current().nextInt(carriers.size())))
                                .put("status", statuses.get(ThreadLocalRandom.current().nextInt(statuses.size())))
                                .put("estimated_delivery", Instant.now().plusSeconds(
                                                ThreadLocalRandom.current().nextInt(1, 6) * 86400L).toString())
                                .put("cost_components", costComponents)
                                .put("route_optimization_score",
                                                round(ThreadLocalRandom.current().nextFloat() * 0.25f + 0.7f, 2))
                                .put("carbon_footprint", round(ThreadLocalRandom.current().nextFloat() * 3f + 2.5f, 2))
                                .put("timestamp", Instant.now().toString());
        }

        private float round(float value, int places) {
                float scale = (float) Math.pow(10, places);
                return Math.round(value * scale) / scale;
        }

        @Override
        public void stop() {
                log.info("Stopping Retail Data Simulator Task");
        }

        private Map<String, String> sourcePartition() {
                Map<String, String> partition = new HashMap<>();
                partition.put("source", "retail-data-simulator");
                return partition;
        }

        private Map<String, Object> sourceOffset() {
                Map<String, Object> offset = new HashMap<>();
                offset.put(POSITION_FIELD, currentPosition.get());
                return offset;
        }
}