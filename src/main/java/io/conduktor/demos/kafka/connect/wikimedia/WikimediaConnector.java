package io.conduktor.demos.kafka.connect.wikimedia;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WikimediaConnector extends SourceConnector {

    public static final String VERSION = "1.1.0";

    public static final String TOPIC_CONFIG = "topic";
    public static final String URL_CONFIG = "url";
    public static final String RECONNECT_TIME_CONFIG = "reconnect.duration";
    public static final String USER_AGENT_CONFIG = "http.headers.user.agent";

    private static final String DEFAULT_USER_AGENT = "WikimediaKafkaConnector/1.0 (https://github.com/conduktor/kafka-connect-wikimedia)";
    private static final int DEFAULT_RECONNECT_MS = 3000;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    "The topic to publish data to")
            .define(URL_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    "The event stream URL to fetch events from")
            .define(RECONNECT_TIME_CONFIG,
                    ConfigDef.Type.INT,
                    DEFAULT_RECONNECT_MS,
                    ConfigDef.Range.atLeast(100),
                    ConfigDef.Importance.LOW,
                    "Reconnect duration (milliseconds) in case of error. Defaults to 3000ms")
            .define(USER_AGENT_CONFIG,
                    ConfigDef.Type.STRING,
                    DEFAULT_USER_AGENT,
                    ConfigDef.Importance.MEDIUM,
                    "User-Agent header value for HTTP requests. Required by Wikimedia API.");

    private String topic;
    private String url;
    private int reconnectDuration;
    private String userAgent;

    @Override
    public void start(Map<String, String> props) {
        // Validate config using ConfigDef
        try {
            CONFIG_DEF.parse(props);
        } catch (ConfigException e) {
            throw new ConfigException("Invalid configuration: " + e.getMessage());
        }

        topic = props.get(TOPIC_CONFIG);
        url = props.get(URL_CONFIG);
        reconnectDuration = Integer.parseInt(props.getOrDefault(RECONNECT_TIME_CONFIG, String.valueOf(DEFAULT_RECONNECT_MS)));
        userAgent = props.getOrDefault(USER_AGENT_CONFIG, DEFAULT_USER_AGENT);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return WikimediaTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // This connector only supports a single task since the Wikimedia SSE stream
        // is a single endpoint. Multiple tasks would create duplicate events.
        Map<String, String> config = new HashMap<>();
        config.put(TOPIC_CONFIG, topic);
        config.put(URL_CONFIG, url);
        config.put(RECONNECT_TIME_CONFIG, String.valueOf(reconnectDuration));
        config.put(USER_AGENT_CONFIG, userAgent);
        return Collections.singletonList(config);
    }

    @Override
    public void stop() {
        // No connector-level resources to clean up
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return VERSION;
    }
}
