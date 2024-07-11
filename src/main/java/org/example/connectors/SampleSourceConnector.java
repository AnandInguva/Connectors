package org.example.connectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.*;

// IMPLEMENT A SOURCE CONNECTOR
public class SampleSourceConnector extends SourceConnector {
    private Map<String, String> configProperties;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        configProperties = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SampleSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks;
             i++) {
            taskConfigs.add(new HashMap<>(configProperties));

        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        // Do any cleanup if needed
    }
    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("numTasks", ConfigDef.Type.INT, 10, ConfigDef.Importance.HIGH, "Number of tasks")
                .define("failAfterMillis", ConfigDef.Type.LONG, 5000, ConfigDef.Importance.HIGH, "Time after which to fail a task (ms)");
    }

    // IMPLEMENT A SOURCE TASK
    public static class SampleSourceTask extends SourceTask {
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
}
