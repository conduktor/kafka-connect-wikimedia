package io.conduktor.demos.kafka.connect.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Custom processor that produces to kafka
 */
public class Processor extends EventProcessor {

    private KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private final Properties producerProps;
    private final Duration closeTimeout;

    public Boolean isRunning = false;

    /**
     * construct the processor
     * @param uri the stream uri
     * @param topic the output topic
     * @param producerProps properties for kafka producer
     * @param closeTimeout close timeout
     */
    public Processor(String uri, String topic, Properties producerProps, Duration closeTimeout) {
        super(uri);
        this.topic = topic;
        this.producerProps = producerProps;
        this.closeTimeout = closeTimeout;
    }

    /**
     * handle each message
     */
    @Override
    protected void onEvent(String event, MessageEvent messageEvent) {
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }


    /**
     * start the processor
     * @param reconnectTime reconnect if processor in case of error for this duration
     * @throws Exception throws if already started
     * @return  a promise which the caller can wait on ; this promise will complete once the processor is shutdown
     */
    @Override
    public CompletableFuture<Long> start(Duration reconnectTime) throws Exception {

        if (kafkaProducer != null) {
            throw new Exception("trying to start already started Processor");
        }
        kafkaProducer = new KafkaProducer<>(producerProps);

        CompletableFuture<Long> future = super.start(reconnectTime);
        isRunning = true;
        return future;

    }

    /**
     * shuts down the processor
     * @throws Exception throw if the processor hasn't started
     */
    @Override
    public void shutdown() throws Exception {
        super.shutdown();
    }

    /**
     * on close callback
     */
    @Override
    protected void onClosed() {
        if (kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close(closeTimeout);
            isRunning = false;
        }
    }
}
