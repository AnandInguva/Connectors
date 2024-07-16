package org.example.connectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;

public class SampleSourceTask extends SourceTask {
    private Long failAfterSeconds;
    // Num of tasks to run.
    private long startTime;
    @Override
    public String version() {
        return null;
    }
    public String failTask;
    // Fail the task after these seconds;
    @Override
    public void start(Map<String, String> map) {
        failAfterSeconds = Long.parseLong(map.getOrDefault("failAfterSeconds", "30"));
        startTime = System.currentTimeMillis();
        failTask = map.getOrDefault("failTask", "false");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (elapsedTime >= failAfterSeconds * 1000 & Objects.equals(failTask, "true")) // elapsedTime is in millis
        {
            throw new ConnectException("Task failed after " + failAfterSeconds + " seconds");
        }

        Thread.sleep(1000);
        List<SourceRecord> sourceRecords = new ArrayList<>();
        long time = System.currentTimeMillis();
        SourceRecord sourceRecord = new SourceRecord(
                Collections.singletonMap("source", "dummy"),
                Collections.singletonMap("offset", elapsedTime),
                "dummy-topic",
                null, null, null,
                Schema.STRING_SCHEMA,
                String.format("Element at time: %s", time), System.currentTimeMillis()
        );

        sourceRecords.add(sourceRecord);
        return sourceRecords;
    }

    @Override
    public void stop() {

    }
}
