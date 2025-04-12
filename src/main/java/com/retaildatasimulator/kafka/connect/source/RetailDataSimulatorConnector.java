package com.retaildatasimulator.kafka.connect.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetailDataSimulatorConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(RetailDataSimulatorConnector.class);

    private Map<String, String> configProps;

    @Override
    public String version() {
        return "0.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
        log.info("Starting Retail Data Simulator Connector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RetailDataSimulatorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(configProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Stopping Retail Data Simulator Connector");
    }

    @Override
    public ConfigDef config() {
        return RetailDataSimulatorConfig.config();
    }
}