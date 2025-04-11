package com.example.kafka.connect.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Configuration class for Example Source Connector.
 * This class defines all configuration properties that the connector accepts.
 */
public class ExampleSourceConnectorConfig extends AbstractConfig {

    // Define common configuration properties
    public static final String TOPIC_CONFIG = "topic";
    public static final String TOPIC_DOC = "Topic to write data to";

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    public static final long POLL_INTERVAL_MS_DEFAULT = 5000;
    public static final String POLL_INTERVAL_MS_DOC = "Frequency in milliseconds to poll for new data";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final int BATCH_SIZE_DEFAULT = 100;
    public static final String BATCH_SIZE_DOC = "Maximum number of records to include in a single batch";

    // The ConfigDef defines the configuration options, their types, defaults, and
    // documentation
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Common configs
            .define(TOPIC_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, TOPIC_DOC)
            .define(POLL_INTERVAL_MS_CONFIG, Type.LONG, POLL_INTERVAL_MS_DEFAULT, Importance.MEDIUM,
                    POLL_INTERVAL_MS_DOC)
            .define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT, Importance.MEDIUM, BATCH_SIZE_DOC);

    public ExampleSourceConnectorConfig(final Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    public static ConfigDef config() {
        return CONFIG_DEF;
    }

    // Convenience methods to access configuration values
    public String getTopic() {
        return getString(TOPIC_CONFIG);
    }

    public long getPollIntervalMs() {
        return getLong(POLL_INTERVAL_MS_CONFIG);
    }

    public int getBatchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }
}
