package io.conduktor.demos.kafka.connect.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Standalone processor that produces directly to Kafka.
 * NOTE: This is NOT used by Kafka Connect - it's for standalone CLI usage only.
 * For Kafka Connect, use WikimediaConnector/WikimediaTask instead.
 */
public class Processor extends EventProcessor {

    private static final Logger log = LoggerFactory.getLogger(Processor.class);

    private KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private final Properties producerProps;
    private final Duration closeTimeout;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public Processor(String uri, String topic, Properties producerProps, Duration closeTimeout) {
        super(uri);
        this.topic = topic;
        this.producerProps = producerProps;
        this.closeTimeout = closeTimeout;
    }

    @Override
    protected void onEvent(String event, MessageEvent messageEvent) {
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public CompletableFuture<Long> start(Duration reconnectTime) {
        if (kafkaProducer != null) {
            throw new IllegalStateException("Processor already started");
        }
        kafkaProducer = new KafkaProducer<>(producerProps);
        isRunning.set(true);
        log.info("Processor started for topic: {}", topic);
        return super.start(reconnectTime);
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    protected void onClosed() {
        if (kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close(closeTimeout);
            isRunning.set(false);
            log.info("Processor closed");
        }
    }

    public boolean isRunning() {
        return isRunning.get();
    }
}
