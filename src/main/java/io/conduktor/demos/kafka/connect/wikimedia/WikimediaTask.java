package io.conduktor.demos.kafka.connect.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class WikimediaTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(WikimediaTask.class);
    private static final int MAX_QUEUE_SIZE = 10000;
    private static final Map<String, String> SOURCE_PARTITION = Collections.singletonMap("source", "wikimedia");

    private EventProcessor processor;
    private BlockingQueue<SourceRecord> queue;
    private String topic;

    @Override
    public String version() {
        return WikimediaConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(WikimediaConnector.TOPIC_CONFIG);
        String uri = props.get(WikimediaConnector.URL_CONFIG);
        int reconnectMs = Integer.parseInt(props.get(WikimediaConnector.RECONNECT_TIME_CONFIG));
        String userAgent = props.get(WikimediaConnector.USER_AGENT_CONFIG);

        queue = new LinkedBlockingDeque<>(MAX_QUEUE_SIZE);

        processor = new EventProcessor(uri, userAgent) {
            @Override
            protected void onClosed() {
                log.info("Wikimedia event stream closed");
            }

            @Override
            protected void onEvent(String event, MessageEvent messageEvent) {
                String lastEventId = messageEvent.getLastEventId();
                Map<String, String> offset = Collections.singletonMap("lastEventId",
                        lastEventId != null ? lastEventId : String.valueOf(System.currentTimeMillis()));

                SourceRecord record = new SourceRecord(
                        SOURCE_PARTITION,
                        offset,
                        topic,
                        Schema.STRING_SCHEMA,
                        messageEvent.getData()
                );

                if (!queue.offer(record)) {
                    log.warn("Queue full, dropping event. Consider increasing consumer throughput.");
                }
            }
        };

        try {
            processor.start(Duration.ofMillis(reconnectMs));
            log.info("WikimediaTask started for topic: {}", topic);
        } catch (Exception e) {
            throw new ConnectException("Failed to start Wikimedia processor", e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new LinkedList<>();

        SourceRecord record = queue.poll(1, TimeUnit.SECONDS);
        if (record == null) {
            return records;
        }

        records.add(record);
        queue.drainTo(records, 500); // Batch up to 500 records

        return records;
    }

    @Override
    public void stop() {
        log.info("Stopping WikimediaTask");
        if (processor != null) {
            processor.shutdown();
        }
    }
}
