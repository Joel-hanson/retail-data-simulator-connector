package com.retaildatasimulator.kafka.connect.source;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class RetailDataSimulatorConfig extends AbstractConfig {
    public static final String INVENTORY_TOPIC_CONFIG = "inventory.topic";
    public static final String INVENTORY_TOPIC_DEFAULT = "inventory_topic";
    public static final String INVENTORY_TOPIC_DOC = "Topic to write inventory events to";

    public static final String ORDERS_TOPIC_CONFIG = "orders.topic";
    public static final String ORDERS_TOPIC_DEFAULT = "orders_topic";
    public static final String ORDERS_TOPIC_DOC = "Topic to write order events to";

    public static final String LOGISTICS_TOPIC_CONFIG = "logistics.topic";
    public static final String LOGISTICS_TOPIC_DEFAULT = "logistics_topic";
    public static final String LOGISTICS_TOPIC_DOC = "Topic to write logistics events to";

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    public static final long POLL_INTERVAL_MS_DEFAULT = 1000L;
    public static final String POLL_INTERVAL_MS_DOC = "Interval in milliseconds between polls";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final int BATCH_SIZE_DEFAULT = 1;
    public static final String BATCH_SIZE_DOC = "Number of records to return in each poll";

    public RetailDataSimulatorConfig(Map<String, String> props) {
        super(config(), props);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(INVENTORY_TOPIC_CONFIG, Type.STRING, INVENTORY_TOPIC_DEFAULT,
                        Importance.HIGH, INVENTORY_TOPIC_DOC)
                .define(ORDERS_TOPIC_CONFIG, Type.STRING, ORDERS_TOPIC_DEFAULT,
                        Importance.HIGH, ORDERS_TOPIC_DOC)
                .define(LOGISTICS_TOPIC_CONFIG, Type.STRING, LOGISTICS_TOPIC_DEFAULT,
                        Importance.HIGH, LOGISTICS_TOPIC_DOC)
                .define(POLL_INTERVAL_MS_CONFIG, Type.LONG, POLL_INTERVAL_MS_DEFAULT,
                        Importance.MEDIUM, POLL_INTERVAL_MS_DOC)
                .define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT,
                        Importance.MEDIUM, BATCH_SIZE_DOC);
    }

    public String getInventoryTopic() {
        return this.getString(INVENTORY_TOPIC_CONFIG);
    }

    public String getOrdersTopic() {
        return this.getString(ORDERS_TOPIC_CONFIG);
    }

    public String getLogisticsTopic() {
        return this.getString(LOGISTICS_TOPIC_CONFIG);
    }

    public long getPollIntervalMs() {
        return this.getLong(POLL_INTERVAL_MS_CONFIG);
    }

    public int getBatchSize() {
        return this.getInt(BATCH_SIZE_CONFIG);
    }
}