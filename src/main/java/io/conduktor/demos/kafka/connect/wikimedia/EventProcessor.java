package io.conduktor.demos.kafka.connect.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * abstract event processor
 */
public abstract class EventProcessor {

    EventProcessor _eventProcessor = this;

    CompletableFuture<Long> promise;

    private Long count = 0L;

    private final String uri;

    private EventSource eventSource;
    public EventProcessor(String uri) {
        this.uri = uri;
    }

    /**
     * override this method to do on close callback
     */
    protected abstract void onClosed();


    /**
     * override this method to handle each message
     */
    protected abstract void onEvent(String event, MessageEvent messageEvent);

    /**
     * shuts down the processor
     * @throws Exception throw if the processor hasn't started
     */
    public void shutdown() throws Exception {
        if (eventSource == null) {
            promise.complete(count);
            throw new Exception("trying to stop EventProcessor without starting");
        } else {
            eventSource.close();
        }
    }

    /**
     * start the processor
     * @param reconnectTime reconnect if processor in case of error for this duration
     * @throws Exception throws if already started
     * @return a promise which the caller can wait on ; this promise will complete once the processor is shutdown
     */
    public CompletableFuture<Long> start(Duration reconnectTime) throws Exception {
        if (eventSource != null) {
            throw new Exception("trying to start already started EventProcessor");
        }
        if (reconnectTime == null) {
            reconnectTime = Duration.ofMillis(3000);
        }
        eventSource = new EventSource.Builder(new CustomEventHandler(), URI.create(this.uri))
                .reconnectTime(reconnectTime).build();
        promise = new CompletableFuture<>();

        eventSource.start();

        return promise;

    }

    /**
     * a custom event handler that delegates to processor methods
     */
    public class CustomEventHandler implements EventHandler {

        @Override
        public void onOpen() { }

        @Override
        public void onClosed() {
            _eventProcessor.onClosed();
            promise.complete(count);
        }

        @Override
        public void onMessage(String event, MessageEvent messageEvent) {
            count++;
            onEvent(event, messageEvent);
        }

        @Override
        public void onComment(String comment) {}

        @Override
        public void onError(Throwable t) {
            System.out.println("onError");
            t.printStackTrace();
        }

    }



}
