package io.conduktor.demos.kafka.connect.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
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
    private final String userAgent;

    private EventSource eventSource;

    /**
     * EventProcessor constructor
     * @param uri event stream URI
     */
    public EventProcessor(String uri) {
        this.uri = uri;
        this.userAgent = null;
    }

    /**
     * EventProcessor constructor with User-Agent support
     * @param uri event stream URI
     * @param userAgent User-Agent header value for HTTP requests
     */
    public EventProcessor(String uri, String userAgent) {
        this.uri = uri;
        this.userAgent = userAgent;
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

        EventSource.Builder builder = new EventSource.Builder(new CustomEventHandler(), URI.create(this.uri))
                .reconnectTime(reconnectTime);

        // Add User-Agent header via OkHttpClient interceptor if specified
        if (userAgent != null && !userAgent.isEmpty()) {
            OkHttpClient okHttpClient = new OkHttpClient.Builder()
                    .addInterceptor(new UserAgentInterceptor(userAgent))
                    .build();
            builder.client(okHttpClient);
        }

        eventSource = builder.build();
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

    /**
     * Interceptor for adding User-Agent header to HTTP requests
     */
    private static class UserAgentInterceptor implements Interceptor {
        private final String userAgent;

        /**
         * Interceptor constructor
         * @param userAgent User-Agent header value
         */
        UserAgentInterceptor(String userAgent) {
            this.userAgent = userAgent;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            Request originalRequest = chain.request();
            Request requestWithUserAgent = originalRequest.newBuilder()
                    .header("User-Agent", userAgent)
                    .build();
            return chain.proceed(requestWithUserAgent);
        }
    }

}
