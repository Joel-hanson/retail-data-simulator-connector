package com.retaildatasimulator.kafka.connect.source;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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

        // Event counters
        private AtomicLong inventoryEventCount = new AtomicLong(0);
        private AtomicLong orderEventCount = new AtomicLong(0);
        private AtomicLong logisticsEventCount = new AtomicLong(0);
        private long lastReportTime = System.currentTimeMillis();
        private static final long REPORT_INTERVAL_MS = 60000; // Report stats every minute

        // Schemas
        private Schema inventorySchema;
        private Schema orderSchema;
        private Schema orderItemSchema;
        private Schema logisticsSchema;
        private Schema costComponentSchema;

        // More realistic data generators
        private final List<String> shoeModels = Arrays.asList(
                        "Air Max 270", "Air Max 90", "Air Max 97", "Air Force 1", "React Element 55",
                        "ZoomX Vaporfly", "Air Jordan 1", "Dunk Low", "Blazer Mid", "Pegasus 39");

        private final List<String> shoeColors = Arrays.asList(
                        "Black/White", "White/Red", "Blue/Navy", "Triple Black", "University Blue",
                        "Volt/Black", "Bred", "Obsidian", "Tan/Brown", "Gray/White");

        private final List<String> suppliers = Arrays.asList(
                        "Nike Direct", "Foot Locker", "JD Sports", "Finish Line", "Dick's Sporting Goods",
                        "Eastbay", "DTLR", "Hibbett Sports", "Shoe Palace", "Sneaker Politics");

        private final List<String> carriers = Arrays.asList(
                        "FedEx", "UPS", "USPS", "DHL", "OnTrac", "LaserShip", "Amazon Logistics");

        private final List<String> statuses = Arrays.asList(
                        "in_transit", "delayed", "out_for_delivery", "delivered", "exception",
                        "pending_pickup", "processing", "customs_clearance");

        private final List<String> paymentMethods = Arrays.asList(
                        "credit_card", "paypal", "apple_pay", "google_pay", "klarna", "afterpay",
                        "store_credit", "gift_card", "cash_on_delivery");

        private final List<String> cities = Arrays.asList(
                        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
                        "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
                        "Fort Worth", "Columbus", "San Francisco", "Charlotte", "Indianapolis", "Seattle");

        private final List<String> eventTypes = Arrays.asList(
                        "inventory_update", "inventory_adjustment", "restock", "shrinkage_report",
                        "return_to_inventory");

        private final List<String> orderEventTypes = Arrays.asList(
                        "order_created", "order_confirmed", "order_processed", "order_shipped", "order_delivered",
                        "order_canceled", "order_returned", "payment_failed");

        private final List<String> logisticsEventTypes = Arrays.asList(
                        "shipment_created", "shipment_update", "delivery_exception", "delivery_attempt",
                        "delivery_successful", "route_optimized", "warehouse_departure");
        private static final List<String> streetNames = Arrays.asList(
                        "Maple", "Oak", "Pine", "Cedar", "Elm", "Washington", "Lake", "Hill", "Main", "Sunset",
                        "Ridge", "River", "Forest", "Adams", "Lincoln", "Jackson", "Franklin", "Jefferson", "Madison");

        private static final List<String> streetSuffixes = Arrays.asList(
                        "St", "Ave", "Blvd", "Rd", "Ln", "Dr", "Ct", "Pl", "Way", "Terrace");

        // Store customer IDs to simulate return customers
        private Map<String, String> customerDatabase = new ConcurrentHashMap<>();
        private static final int MAX_CUSTOMERS = 1000;

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
                initializeCustomerDatabase();

                offset = context.offsetStorageReader().offset(sourcePartition());
                if (offset != null && offset.containsKey(POSITION_FIELD)) {
                        currentPosition.set((Long) offset.get(POSITION_FIELD));
                        log.info("Loaded offset: {}", currentPosition.get());
                } else {
                        currentPosition.set(0L);
                        log.info("No stored offset, starting from 0");
                }
        }

        private void initializeCustomerDatabase() {
                // Pre-populate customer database with some customers
                for (int i = 0; i < 200; i++) {
                        String customerId = UUID.randomUUID().toString();
                        customerDatabase.put("customer-" + i, customerId);
                }
        }

        private void initializeSchemas() {
                // Order Item Schema
                orderItemSchema = SchemaBuilder.struct()
                                .name("OrderItem")
                                .field("sku", Schema.STRING_SCHEMA)
                                .field("model_name", Schema.STRING_SCHEMA).optional()
                                .field("color", Schema.STRING_SCHEMA).optional()
                                .field("quantity", Schema.INT32_SCHEMA).optional()
                                .field("price", Schema.FLOAT32_SCHEMA).optional()
                                .field("size", Schema.STRING_SCHEMA).optional()
                                .field("is_discounted", Schema.BOOLEAN_SCHEMA).optional()
                                .field("discount_pct", Schema.OPTIONAL_FLOAT32_SCHEMA).optional()
                                .build();

                // Order Schema
                orderSchema = SchemaBuilder.struct()
                                .name("OrderEvent")
                                .field("event_id", Schema.STRING_SCHEMA)
                                .field("event_type", Schema.STRING_SCHEMA).optional()
                                .field("order_id", Schema.STRING_SCHEMA).optional()
                                .field("customer_id", Schema.STRING_SCHEMA).optional()
                                .field("items", SchemaBuilder.array(orderItemSchema)).optional()
                                .field("total_amount", Schema.FLOAT32_SCHEMA).optional()
                                .field("subtotal", Schema.FLOAT32_SCHEMA).optional()
                                .field("tax_amount", Schema.FLOAT32_SCHEMA).optional()
                                .field("shipping_amount", Schema.FLOAT32_SCHEMA).optional()
                                .field("payment_method", Schema.STRING_SCHEMA).optional()
                                .field("payment_details", Schema.STRING_SCHEMA).optional()
                                .field("loyalty_points_used", Schema.INT32_SCHEMA).optional()
                                .field("loyalty_points_earned", Schema.INT32_SCHEMA).optional()
                                .field("shipping_address", Schema.STRING_SCHEMA).optional()
                                .field("billing_address", Schema.STRING_SCHEMA).optional()
                                .field("is_gift", Schema.BOOLEAN_SCHEMA).optional()
                                .field("timestamp", Schema.INT64_SCHEMA).optional()
                                .build();

                // Inventory Schema
                inventorySchema = SchemaBuilder.struct()
                                .name("InventoryEvent")
                                .field("event_id", Schema.STRING_SCHEMA)
                                .field("event_type", Schema.STRING_SCHEMA).optional()
                                .field("location", Schema.STRING_SCHEMA).optional()
                                .field("location_type", Schema.STRING_SCHEMA).optional()
                                .field("sku", Schema.STRING_SCHEMA).optional()
                                .field("model_name", Schema.STRING_SCHEMA).optional()
                                .field("color", Schema.STRING_SCHEMA).optional()
                                .field("size", Schema.STRING_SCHEMA).optional()
                                .field("quantity", Schema.INT32_SCHEMA).optional()
                                .field("timestamp", Schema.INT64_SCHEMA).optional()
                                .field("reorder_threshold", Schema.INT32_SCHEMA).optional()
                                .field("supplier", Schema.STRING_SCHEMA).optional()
                                .field("last_restock_date", Schema.STRING_SCHEMA).optional()
                                .field("days_in_stock", Schema.INT32_SCHEMA).optional()
                                .build();

                // Cost Component Schema
                costComponentSchema = SchemaBuilder.struct()
                                .name("CostComponent")
                                .field("name", Schema.STRING_SCHEMA).optional()
                                .field("value", Schema.FLOAT32_SCHEMA).optional()
                                .build();

                // Logistics Schema
                logisticsSchema = SchemaBuilder.struct()
                                .name("LogisticsEvent")
                                .field("event_id", Schema.STRING_SCHEMA)
                                .field("event_type", Schema.STRING_SCHEMA).optional()
                                .field("shipment_id", Schema.STRING_SCHEMA).optional()
                                .field("order_id", Schema.STRING_SCHEMA).optional()
                                .field("carrier", Schema.STRING_SCHEMA).optional()
                                .field("status", Schema.STRING_SCHEMA).optional()
                                .field("shipping_method", Schema.STRING_SCHEMA).optional()
                                .field("origin", Schema.STRING_SCHEMA).optional()
                                .field("destination", Schema.STRING_SCHEMA).optional()
                                .field("estimated_delivery", Schema.STRING_SCHEMA).optional()
                                .field("actual_delivery", Schema.OPTIONAL_STRING_SCHEMA).optional()
                                .field("cost_components", SchemaBuilder.array(costComponentSchema)).optional()
                                .field("package_weight", Schema.FLOAT32_SCHEMA).optional()
                                .field("package_dimensions", Schema.STRING_SCHEMA).optional()
                                .field("timestamp", Schema.INT64_SCHEMA).optional()
                                .field("total_shipping_cost", Schema.FLOAT32_SCHEMA).optional()
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
                                inventoryEventCount.incrementAndGet();

                                // Generate order event
                                Struct orderEvent = generateOrderEvent();
                                records.add(new SourceRecord(
                                                sourcePartition(),
                                                sourceOffset(),
                                                ordersTopic,
                                                null,
                                                orderSchema,
                                                orderEvent));
                                orderEventCount.incrementAndGet();

                                // Generate logistics event
                                Struct logisticsEvent = generateLogisticsEvent();
                                records.add(new SourceRecord(
                                                sourcePartition(),
                                                sourceOffset(),
                                                logisticsTopic,
                                                null,
                                                logisticsSchema,
                                                logisticsEvent));
                                logisticsEventCount.incrementAndGet();

                                currentPosition.incrementAndGet();
                        }

                        // Report event counts periodically
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastReportTime > REPORT_INTERVAL_MS) {
                                reportEventCounts();
                                lastReportTime = currentTime;
                        }

                } catch (Exception e) {
                        log.error("Error generating data", e);
                }

                if (!records.isEmpty()) {
                        log.debug("Generated {} records ({} batches)", records.size(), batchSize);
                }

                return records;
        }

        private void reportEventCounts() {
                log.info("=== EVENT COUNT SUMMARY ===");
                log.info("Total Inventory Events: {}", inventoryEventCount.get());
                log.info("Total Order Events: {}", orderEventCount.get());
                log.info("Total Logistics Events: {}", logisticsEventCount.get());
                log.info("Total Events: {}",
                                inventoryEventCount.get() + orderEventCount.get() + logisticsEventCount.get());
                log.info("===========================");
        }

        private Struct generateInventoryEvent() {
                // Generate more realistic SKU
                String model = shoeModels.get(ThreadLocalRandom.current().nextInt(shoeModels.size()));
                String color = shoeColors.get(ThreadLocalRandom.current().nextInt(shoeColors.size()));
                String size = String.valueOf(ThreadLocalRandom.current().nextInt(6, 14));
                // Add half sizes
                if (ThreadLocalRandom.current().nextBoolean()) {
                        size += ".5";
                }

                String sku = model.replace(" ", "-") + "-" + color.replace("/", "-") + "-" + size;
                String locationType = ThreadLocalRandom.current().nextBoolean() ? "store" : "warehouse";
                String location = cities.get(ThreadLocalRandom.current().nextInt(cities.size()));

                // Calculate last restock date (between 1-60 days ago)
                int daysInStock = ThreadLocalRandom.current().nextInt(1, 61);
                LocalDateTime lastRestockDate = LocalDateTime.now().minusDays(daysInStock);

                String eventType = eventTypes.get(ThreadLocalRandom.current().nextInt(eventTypes.size()));

                return new Struct(inventorySchema)
                                .put("event_id", UUID.randomUUID().toString())
                                .put("event_type", eventType)
                                .put("location", location)
                                .put("location_type", locationType)
                                .put("sku", sku)
                                .put("model_name", model)
                                .put("color", color)
                                .put("size", size)
                                .put("quantity", ThreadLocalRandom.current().nextInt(-5, 101)) // Allow negative for
                                                                                               // adjustments
                                .put("timestamp", generateTimestamp())
                                .put("reorder_threshold", ThreadLocalRandom.current().nextInt(5, 26))
                                .put("supplier", suppliers.get(ThreadLocalRandom.current().nextInt(suppliers.size())))
                                .put("last_restock_date", formatDate(lastRestockDate))
                                .put("days_in_stock", daysInStock);
        }

        private Struct generateOrderEvent() {
                // Select a random customer (80% chance of returning customer, 20% chance of new
                // customer)
                String customerId;
                if (ThreadLocalRandom.current().nextDouble() < 0.8 && !customerDatabase.isEmpty()) {
                        // Return customer
                        List<String> customerKeys = new ArrayList<>(customerDatabase.keySet());
                        String randomKey = customerKeys.get(ThreadLocalRandom.current().nextInt(customerKeys.size()));
                        customerId = customerDatabase.get(randomKey);
                } else {
                        // New customer
                        customerId = UUID.randomUUID().toString();
                        if (customerDatabase.size() < MAX_CUSTOMERS) {
                                customerDatabase.put("customer-" + customerDatabase.size(), customerId);
                        }
                }

                // Generate between 1-5 items per order with weighted probability
                int itemCount;
                double rand = ThreadLocalRandom.current().nextDouble();
                if (rand < 0.5) {
                        itemCount = 1; // 50% chance of 1 item
                } else if (rand < 0.8) {
                        itemCount = 2; // 30% chance of 2 items
                } else if (rand < 0.95) {
                        itemCount = 3; // 15% chance of 3 items
                } else {
                        itemCount = ThreadLocalRandom.current().nextInt(4, 6); // 5% chance of 4-5 items
                }

                List<Struct> items = IntStream.range(0, itemCount)
                                .mapToObj(i -> {
                                        String model = shoeModels
                                                        .get(ThreadLocalRandom.current().nextInt(shoeModels.size()));
                                        String color = shoeColors
                                                        .get(ThreadLocalRandom.current().nextInt(shoeColors.size()));
                                        String size = String.valueOf(ThreadLocalRandom.current().nextInt(6, 14));
                                        // Add half sizes
                                        if (ThreadLocalRandom.current().nextBoolean()) {
                                                size += ".5";
                                        }

                                        String sku = model.replace(" ", "-") + "-" + color.replace("/", "-") + "-"
                                                        + size;

                                        // Calculate price based on model with some randomness
                                        float basePrice = 0;
                                        if (model.contains("Air Max"))
                                                basePrice = 130f;
                                        else if (model.contains("Air Force"))
                                                basePrice = 100f;
                                        else if (model.contains("Jordan"))
                                                basePrice = 180f;
                                        else if (model.contains("ZoomX"))
                                                basePrice = 250f;
                                        else if (model.contains("React"))
                                                basePrice = 120f;
                                        else if (model.contains("Dunk"))
                                                basePrice = 110f;
                                        else if (model.contains("Blazer"))
                                                basePrice = 95f;
                                        else if (model.contains("Pegasus"))
                                                basePrice = 120f;
                                        else
                                                basePrice = 110f;

                                        // Add some price variation (+/- 10%)
                                        basePrice += ThreadLocalRandom.current().nextFloat() * 20f - 10f;

                                        // Some items may be discounted
                                        boolean isDiscounted = ThreadLocalRandom.current().nextFloat() < 0.2f; // 20%
                                                                                                               // chance
                                        Float discountPct = null;

                                        if (isDiscounted) {
                                                // Discounts of 10%, 15%, 20%, 25%, or 30%
                                                int[] discounts = { 10, 15, 20, 25, 30 };
                                                discountPct = (float) discounts[ThreadLocalRandom.current()
                                                                .nextInt(discounts.length)];
                                                basePrice = basePrice * (1 - discountPct / 100);
                                        }

                                        return new Struct(orderItemSchema)
                                                        .put("sku", sku)
                                                        .put("model_name", model)
                                                        .put("color", color)
                                                        .put("quantity", ThreadLocalRandom.current().nextInt(1, 3))
                                                        .put("price", round(basePrice, 2))
                                                        .put("size", size)
                                                        .put("is_discounted", isDiscounted)
                                                        .put("discount_pct", discountPct);
                                })
                                .collect(Collectors.toList());

                String paymentMethod = paymentMethods.get(ThreadLocalRandom.current().nextInt(paymentMethods.size()));

                // Generate realistic payment details based on method
                String paymentDetails;
                switch (paymentMethod) {
                        case "credit_card":
                                String[] cardTypes = { "Visa", "MasterCard", "Amex", "Discover" };
                                String cardType = cardTypes[ThreadLocalRandom.current().nextInt(cardTypes.length)];
                                paymentDetails = cardType + " ****" + ThreadLocalRandom.current().nextInt(1000, 10000);
                                break;
                        case "paypal":
                                paymentDetails = "PayPal Transaction ID: "
                                                + UUID.randomUUID().toString().substring(0, 10);
                                break;
                        case "apple_pay":
                                paymentDetails = "Apple Pay Transaction";
                                break;
                        case "google_pay":
                                paymentDetails = "Google Pay Transaction";
                                break;
                        case "klarna":
                                paymentDetails = "Klarna - Pay in 4";
                                break;
                        case "afterpay":
                                paymentDetails = "Afterpay - 4 interest-free payments";
                                break;
                        case "gift_card":
                                paymentDetails = "Gift Card Balance: $" + ThreadLocalRandom.current().nextInt(50, 201);
                                break;
                        default:
                                paymentDetails = paymentMethod + " payment confirmed";
                }

                // Calculate order totals
                float subtotal = (float) items.stream()
                                .mapToDouble(item -> item.getInt32("quantity") * item.getFloat32("price"))
                                .sum();

                // Calculate tax (5-10% based on location)
                float taxRate = ThreadLocalRandom.current().nextFloat() * 0.05f + 0.05f;
                float taxAmount = round(subtotal * taxRate, 2);

                // Calculate shipping (free for orders above $100, otherwise $5-15)
                float shippingAmount = (subtotal > 100) ? 0f
                                : round(ThreadLocalRandom.current().nextFloat() * 10f + 5f, 2);

                float totalAmount = round(subtotal + taxAmount + shippingAmount, 2);

                // Generate random city for address
                String city = cities.get(ThreadLocalRandom.current().nextInt(cities.size()));
                String[] states = { "CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI" };
                String state = states[ThreadLocalRandom.current().nextInt(states.length)];
                String address = ThreadLocalRandom.current().nextInt(100, 10000) + " " +
                                getRandomStreetName() + ", " + city + ", " + state + " " +
                                ThreadLocalRandom.current().nextInt(10000, 100000);

                // 90% chance shipping and billing address are the same
                String billingAddress = ThreadLocalRandom.current().nextFloat() < 0.9f ? address
                                : ThreadLocalRandom.current().nextInt(100, 10000) + " " +
                                                getRandomStreetName() + ", "
                                                + cities.get(ThreadLocalRandom.current().nextInt(cities.size())) +
                                                ", " + states[ThreadLocalRandom.current().nextInt(states.length)] + " "
                                                +
                                                ThreadLocalRandom.current().nextInt(10000, 100000);

                // Generate if this is a gift (10% chance)
                boolean isGift = ThreadLocalRandom.current().nextFloat() < 0.1f;

                // Calculate loyalty points
                int loyaltyPointsUsed = ThreadLocalRandom.current().nextFloat() < 0.3f
                                ? ThreadLocalRandom.current().nextInt(0, 501)
                                : 0;
                int loyaltyPointsEarned = (int) (totalAmount * ThreadLocalRandom.current().nextFloat() * 3f + 1f);

                // Generate order event type with proper distribution
                String eventType;
                double eventTypeProbability = ThreadLocalRandom.current().nextDouble();
                if (eventTypeProbability < 0.6) {
                        eventType = "order_created"; // 60% new orders
                } else if (eventTypeProbability < 0.8) {
                        eventType = "order_shipped"; // 20% shipped
                } else if (eventTypeProbability < 0.9) {
                        eventType = "order_delivered"; // 10% delivered
                } else if (eventTypeProbability < 0.95) {
                        eventType = "order_canceled"; // 5% canceled
                } else {
                        eventType = orderEventTypes.get(ThreadLocalRandom.current().nextInt(orderEventTypes.size())); // 5%
                                                                                                                      // other
                                                                                                                      // types
                }

                String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

                return new Struct(orderSchema)
                                .put("event_id", UUID.randomUUID().toString())
                                .put("event_type", eventType)
                                .put("order_id", orderId)
                                .put("customer_id", customerId)
                                .put("items", items)
                                .put("subtotal", round(subtotal, 2))
                                .put("tax_amount", taxAmount)
                                .put("shipping_amount", shippingAmount)
                                .put("total_amount", totalAmount)
                                .put("payment_method", paymentMethod)
                                .put("payment_details", paymentDetails)
                                .put("loyalty_points_used", loyaltyPointsUsed)
                                .put("loyalty_points_earned", loyaltyPointsEarned)
                                .put("shipping_address", address)
                                .put("billing_address", billingAddress)
                                .put("is_gift", isGift)
                                .put("timestamp", generateTimestamp());
        }

        private Struct generateLogisticsEvent() {
                // Create more realistic logistics event
                String shipmentId = "SHIP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
                String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
                String carrier = carriers.get(ThreadLocalRandom.current().nextInt(carriers.size()));
                String status = statuses.get(ThreadLocalRandom.current().nextInt(statuses.size()));

                // Choose shipping method
                String[] shippingMethods = { "Standard", "Express", "Next Day", "Two-Day", "International" };
                String shippingMethod = shippingMethods[ThreadLocalRandom.current().nextInt(shippingMethods.length)];

                // Calculate delivery estimate based on shipping method
                int deliveryDays;
                switch (shippingMethod) {
                        case "Next Day":
                                deliveryDays = 1;
                                break;
                        case "Two-Day":
                                deliveryDays = 2;
                                break;
                        case "Express":
                                deliveryDays = ThreadLocalRandom.current().nextInt(2, 4);
                                break;
                        case "International":
                                deliveryDays = ThreadLocalRandom.current().nextInt(5, 11);
                                break;
                        default:
                                deliveryDays = ThreadLocalRandom.current().nextInt(3, 6); // Standard
                }

                LocalDateTime estimatedDelivery = LocalDateTime.now().plusDays(deliveryDays);
                String estimatedDeliveryStr = formatDate(estimatedDelivery);

                // Only set actual delivery if status is delivered
                String actualDelivery = null;
                if (status.equals("delivered")) {
                        LocalDateTime actualDeliveryDate = estimatedDelivery.plusDays(
                                        ThreadLocalRandom.current().nextInt(-1, 2)); // -1 to +1 day from estimate
                        actualDelivery = formatDate(actualDeliveryDate);
                }

                // Set origin and destination
                String origin = cities.get(ThreadLocalRandom.current().nextInt(cities.size()));
                String destination = cities.get(ThreadLocalRandom.current().nextInt(cities.size()));

                // Generate event type based on status
                String eventType;
                switch (status) {
                        case "in_transit":
                                eventType = "shipment_update";
                                break;
                        case "delayed":
                                eventType = "delivery_exception";
                                break;
                        case "out_for_delivery":
                                eventType = "shipment_update";
                                break;
                        case "delivered":
                                eventType = "delivery_successful";
                                break;
                        case "exception":
                                eventType = "delivery_exception";
                                break;
                        case "pending_pickup":
                                eventType = "shipment_created";
                                break;
                        case "processing":
                                eventType = "warehouse_departure";
                                break;
                        default:
                                eventType = logisticsEventTypes
                                                .get(ThreadLocalRandom.current().nextInt(logisticsEventTypes.size()));
                }

                // Calculate package weight and dimensions
                float packageWeight = 0.0f;
                for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, 4); i++) {
                        // Each shoe weighs between 0.7-1.2 kg
                        packageWeight += ThreadLocalRandom.current().nextFloat() * 0.5f + 0.7f;
                }
                packageWeight = round(packageWeight, 2);

                // Package dimensions in inches (L x W x H)
                int length = ThreadLocalRandom.current().nextInt(12, 16);
                int width = ThreadLocalRandom.current().nextInt(8, 12);
                int height = ThreadLocalRandom.current().nextInt(4, 8);
                String packageDimensions = length + "x" + width + "x" + height;

                // Generate more realistic cost components
                List<Struct> costComponents = new ArrayList<>();

                // Base shipping cost varies by method
                float baseCost;
                switch (shippingMethod) {
                        case "Next Day":
                                baseCost = ThreadLocalRandom.current().nextFloat() * 15f + 25f;
                                break;
                        case "Two-Day":
                                baseCost = ThreadLocalRandom.current().nextFloat() * 10f + 15f;
                                break;
                        case "Express":
                                baseCost = ThreadLocalRandom.current().nextFloat() * 8f + 12f;
                                break;
                        case "International":
                                baseCost = ThreadLocalRandom.current().nextFloat() * 20f + 35f;
                                break;
                        default:
                                baseCost = ThreadLocalRandom.current().nextFloat() * 5f + 5f; // Standard
                }

                costComponents.add(new Struct(costComponentSchema)
                                .put("name", "base")
                                .put("value", round(baseCost, 2)));

                // Add fuel surcharge (always present)
                costComponents.add(new Struct(costComponentSchema)
                                .put("name", "fuel_surcharge")
                                .put("value", round(baseCost * (ThreadLocalRandom.current().nextFloat() * 0.1f + 0.05f),
                                                2))); // 5-15% of base

                // Add other charges based on probability
                if (ThreadLocalRandom.current().nextFloat() < 0.3f) {
                        costComponents.add(new Struct(costComponentSchema)
                                        .put("name", "oversize_fee")
                                        .put("value", round(ThreadLocalRandom.current().nextFloat() * 10f + 5f, 2)));
                }

                if (ThreadLocalRandom.current().nextFloat() < 0.1f) {
                        costComponents.add(new Struct(costComponentSchema)
                                        .put("name", "priority_handling")
                                        .put("value", round(ThreadLocalRandom.current().nextFloat() * 8f + 7f, 2)));
                }

                if (ThreadLocalRandom.current().nextFloat() < 0.15f) {
                        costComponents.add(new Struct(costComponentSchema)
                                        .put("name", "residential_delivery_fee")
                                        .put("value", round(ThreadLocalRandom.current().nextFloat() * 2f + 3f, 2)));
                }

                if (shippingMethod.equals("International")) {
                        costComponents.add(new Struct(costComponentSchema)
                                        .put("name", "customs_fee")
                                        .put("value", round(ThreadLocalRandom.current().nextFloat() * 15f + 10f, 2)));
                }
                // Total cost calculation
                float totalCost = 0f;
                for (Struct component : costComponents) {
                        totalCost += (float) component.get("value");
                }

                return new Struct(logisticsSchema)
                                .put("event_id", UUID.randomUUID().toString())
                                .put("event_type", eventType)
                                .put("shipment_id", shipmentId)
                                .put("order_id", orderId)
                                .put("carrier", carrier)
                                .put("status", status)
                                .put("shipping_method", shippingMethod)
                                .put("origin", origin)
                                .put("destination", destination)
                                .put("estimated_delivery", estimatedDeliveryStr)
                                .put("actual_delivery", actualDelivery)
                                .put("cost_components", costComponents)
                                .put("package_weight", packageWeight)
                                .put("package_dimensions", packageDimensions)
                                .put("timestamp", generateTimestamp())
                                .put("total_shipping_cost", round(totalCost, 2));
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

        private long generateTimestamp() {
                // Returns current timestamp in milliseconds since epoch
                return Instant.now().toEpochMilli();
        }

        private String formatDate(LocalDateTime dateTime) {
                // Formats the date in ISO_LOCAL_DATE format (e.g., 2025-05-04)
                DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE;
                return dateTime.format(formatter);
        }

        private String getRandomStreetName() {
                String name = streetNames.get(ThreadLocalRandom.current().nextInt(streetNames.size()));
                String suffix = streetSuffixes.get(ThreadLocalRandom.current().nextInt(streetSuffixes.size()));
                int number = ThreadLocalRandom.current().nextInt(100, 9999); // Add a realistic street number
                return number + " " + name + " " + suffix;
        }

}