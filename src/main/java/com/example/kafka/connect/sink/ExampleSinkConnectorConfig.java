package com.example.kafka.connect.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.List;

import java.util.Map;

/**
 * Configuration class for Example Sink Connector.
 * This class defines all configuration properties that the connector accepts.
 */
public class ExampleSinkConnectorConfig extends AbstractConfig {

    // Define common configuration properties
    public static final String TOPICS_CONFIG = "topics";
    public static final String TOPICS_DOC = "List of topics to consume from";

    public static final String MAX_RETRIES_CONFIG = "max.retries";
    public static final int MAX_RETRIES_DEFAULT = 10;
    public static final String MAX_RETRIES_DOC = "Maximum number of retries for failed operations";

    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    public static final long RETRY_BACKOFF_MS_DEFAULT = 3000;
    public static final String RETRY_BACKOFF_MS_DOC = "Backoff time in milliseconds between retries";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final int BATCH_SIZE_DEFAULT = 100;
    public static final String BATCH_SIZE_DOC = "Maximum number of records to include in a single batch";

    // The ConfigDef defines the configuration options, their types, defaults, and
    // documentation
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Common configs
            .define(TOPICS_CONFIG, Type.LIST, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, TOPICS_DOC)
            .define(MAX_RETRIES_CONFIG, Type.INT, MAX_RETRIES_DEFAULT, Importance.MEDIUM, MAX_RETRIES_DOC)
            .define(RETRY_BACKOFF_MS_CONFIG, Type.LONG, RETRY_BACKOFF_MS_DEFAULT, Importance.MEDIUM,
                    RETRY_BACKOFF_MS_DOC)
            .define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT, Importance.MEDIUM, BATCH_SIZE_DOC);

    public ExampleSinkConnectorConfig(final Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    public static ConfigDef config() {
        return CONFIG_DEF;
    }

    // Convenience methods to access configuration values
    public List<String> getTopics() {
        return getList(TOPICS_CONFIG);
    }

    public int getMaxRetries() {
        return getInt(MAX_RETRIES_CONFIG);
    }

    public long getRetryBackoffMs() {
        return getLong(RETRY_BACKOFF_MS_CONFIG);
    }

    public int getBatchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }
}
