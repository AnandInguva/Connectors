package org.example.connectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SampleSourceTask extends SourceTask {
    private Long failAfterMillis;
    // Num of tasks to run.
    private int numTasks;
    private long startTime;
    private int currentTask = 0;
    @Override
    public String version() {
        return null;
    }
    // Fail the task after these seconds;
    @Override
    public void start(Map<String, String> map) {
        failAfterMillis = Long.parseLong(map.getOrDefault("taskFailSeconds", "5000"));
        numTasks = Integer.parseInt(map.getOrDefault("numTasks", "5"));
        startTime = System.currentTimeMillis();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (elapsedTime >= failAfterMillis) {
            throw new ConnectException("Task failed after " + failAfterMillis + " milliseconds");
        }

        Thread.sleep(1000);

        Schema valueSchema = SchemaBuilder.struct().field("task_id", Schema.INT32_SCHEMA).build();

        SourceRecord sourceRecord = new SourceRecord(
                Collections.singletonMap("source", "dummy"),
                Collections.singletonMap("offset", elapsedTime),
                "dummy-topic",
                null, null, null,
                valueSchema,
                Collections.singletonMap("task_id", elapsedTime)
        );

        return Collections.singletonList(sourceRecord);
    }

    @Override
    public void stop() {

    }
}
