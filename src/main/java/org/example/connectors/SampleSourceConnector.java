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
}
