package com.example.kafka.utils;

public class ExampleDataRecord {
    private final long id;
    private final String name;
    private final double value;
    private final long timestamp;

    public ExampleDataRecord(final long id, final String name, final double value, final long timestamp) {
        this.id = id;
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
