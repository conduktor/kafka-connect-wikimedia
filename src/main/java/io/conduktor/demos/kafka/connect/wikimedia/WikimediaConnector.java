package io.conduktor.demos.kafka.connect.wikimedia;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WikimediaConnector extends SourceConnector {

    //static version for the connector
    public static String VERSION = "0.01";

    // define configs name
    public static final String TOPIC_CONFIG = "topic";
    public static final String URL_CONFIG = "url";
    public static final String RECONNECT_TIME_CONFIG = "reconnect.duration";

    //config definition object
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(URL_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "The event stream url to fetch events from")
            .define(RECONNECT_TIME_CONFIG, ConfigDef.Type.INT, null, ConfigDef.Importance.LOW, "reconnect duration (milliseconds) config for event stream url. defaults to 3 seconds");

    private String topic;
    private String uri;
    private String reconnectDuration;


    @Override
    public void start(Map<String, String> map) {
        // init the configs on start
        topic = map.get(WikimediaConnector.TOPIC_CONFIG);
        uri = map.get(WikimediaConnector.URL_CONFIG);
        reconnectDuration = map.getOrDefault(WikimediaConnector.RECONNECT_TIME_CONFIG, "3000");
    }

    @Override
    public Class<WikimediaTask> taskClass() {
        return WikimediaTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {

        //create a config map for each task
        // in this case only one task is relevant
        Map<String, String> config = new HashMap<>();
        config.put(TOPIC_CONFIG, topic);
        config.put(URL_CONFIG, uri);
        config.put(RECONNECT_TIME_CONFIG, reconnectDuration);
        return Collections.singletonList(config);
    }

    @Override
    public void stop() {

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
