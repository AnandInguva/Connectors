package org.example.connectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SampleSourceTask extends SourceTask {
    private Long failAfterMillis;
    // Num of tasks to run.
    private long startTime;@Override
    public String version() {
        return null;
    }
    // Fail the task after these seconds;
    @Override
    public void start(Map<String, String> map) {
        failAfterMillis = Long.parseLong(map.getOrDefault("failAfterMillis", "5000"));
        startTime = System.currentTimeMillis();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (elapsedTime >= failAfterMillis) {
            throw new ConnectException("Task failed after " + failAfterMillis + " milliseconds");
        }

        Thread.sleep(1000);
        List<SourceRecord> sourceRecords = new ArrayList<>();
        SourceRecord sourceRecord = new SourceRecord(
                Collections.singletonMap("source", "dummy"),
                Collections.singletonMap("offset", elapsedTime),
                "dummy-topic",
                null, null, null,
                Schema.STRING_SCHEMA,
                "dummy_element", System.currentTimeMillis()
        );

        sourceRecords.add(sourceRecord);
        return sourceRecords;
    }

    @Override
    public void stop() {

    }
}
