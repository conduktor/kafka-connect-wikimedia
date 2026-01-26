package io.conduktor.demos.kafka.connect.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract event processor for Server-Sent Events streams.
 */
public abstract class EventProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventProcessor.class);

    private final AtomicLong count = new AtomicLong(0);
    private final String uri;
    private final String userAgent;

    private CompletableFuture<Long> promise;
    private EventSource eventSource;
    private OkHttpClient httpClient;

    public EventProcessor(String uri) {
        this.uri = uri;
        this.userAgent = null;
    }

    public EventProcessor(String uri, String userAgent) {
        this.uri = uri;
        this.userAgent = userAgent;
    }

    /**
     * Called when the event source connection is closed.
     */
    protected abstract void onClosed();

    /**
     * Called for each received event.
     */
    protected abstract void onEvent(String event, MessageEvent messageEvent);

    /**
     * Shuts down the processor and releases resources.
     */
    public void shutdown() {
        if (eventSource != null) {
            eventSource.close();
        }
        if (httpClient != null) {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
            try {
                httpClient.dispatcher().executorService().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for HTTP client shutdown");
            }
        }
        if (promise != null && !promise.isDone()) {
            promise.complete(count.get());
        }
    }

    /**
     * Starts the event processor.
     *
     * @param reconnectTime duration to wait before reconnecting on error
     * @return a future that completes when the processor is shut down
     * @throws IllegalStateException if already started
     */
    public CompletableFuture<Long> start(Duration reconnectTime) {
        if (eventSource != null) {
            throw new IllegalStateException("EventProcessor already started");
        }

        Duration reconnect = reconnectTime != null ? reconnectTime : Duration.ofMillis(3000);

        EventSource.Builder builder = new EventSource.Builder(new InternalEventHandler(), URI.create(this.uri))
                .reconnectTime(reconnect);

        if (userAgent != null && !userAgent.isEmpty()) {
            httpClient = new OkHttpClient.Builder()
                    .addInterceptor(new UserAgentInterceptor(userAgent))
                    .build();
            builder.client(httpClient);
        }

        eventSource = builder.build();
        promise = new CompletableFuture<>();

        eventSource.start();
        log.info("EventProcessor started for URI: {}", uri);

        return promise;
    }

    /**
     * Returns the current event count.
     */
    public long getCount() {
        return count.get();
    }

    private class InternalEventHandler implements EventHandler {

        @Override
        public void onOpen() {
            log.debug("EventSource connection opened");
        }

        @Override
        public void onClosed() {
            log.info("EventSource connection closed after {} events", count.get());
            EventProcessor.this.onClosed();
            if (promise != null) {
                promise.complete(count.get());
            }
        }

        @Override
        public void onMessage(String event, MessageEvent messageEvent) {
            count.incrementAndGet();
            onEvent(event, messageEvent);
        }

        @Override
        public void onComment(String comment) {
            // SSE keep-alive comments, ignore
        }

        @Override
        public void onError(Throwable t) {
            log.error("EventSource error: {}", t.getMessage(), t);
        }
    }

    private static class UserAgentInterceptor implements Interceptor {
        private final String userAgent;

        UserAgentInterceptor(String userAgent) {
            this.userAgent = userAgent;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            Request request = chain.request().newBuilder()
                    .header("User-Agent", userAgent)
                    .build();
            return chain.proceed(request);
        }
    }
}
