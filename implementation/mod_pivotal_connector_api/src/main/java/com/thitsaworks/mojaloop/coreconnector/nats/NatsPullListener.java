package com.thitsaworks.mojaloop.coreconnector.nats;

import com.thitsaworks.mojaloop.coreconnector.logging.MdcContext;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class NatsPullListener<T> {

    @FunctionalInterface
    public interface Handler<T> {

        void handle(T message) throws Exception;

    }

    private static final int BATCH_SIZE = 10;
    private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration RECONNECT_WAIT = Duration.ofSeconds(1);
    private static final Duration STOP_WAIT = Duration.ofSeconds(2);

    private final Logger log;
    private final NatsService natsService;
    private final String operationName;
    private final Class<T> messageType;
    private final Handler<T> handler;
    private final Function<T, Map<String, String>> mdcExtractor;

    private volatile boolean running;
    private volatile Thread thread;
    private volatile JetStreamSubscription subscription;

    public NatsPullListener(Logger log,
                            NatsService natsService,
                            String operationName,
                            Class<T> messageType,
                            Handler<T> handler,
                            Function<T, Map<String, String>> mdcExtractor) {

        this.log = log;
        this.natsService = natsService;
        this.operationName = operationName;
        this.messageType = messageType;
        this.handler = handler;
        this.mdcExtractor = mdcExtractor != null ? mdcExtractor : ignored -> Collections.emptyMap();
    }

    public void start(String subject, String stream, String durable, String threadName) {

        running = true;
        thread = new Thread(() -> consume(subject, stream, durable), threadName);
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {

        running = false;

        JetStreamSubscription currentSubscription = subscription;
        if (currentSubscription != null) {
            safeUnsubscribe(currentSubscription);
        }

        Thread currentThread = thread;
        if (currentThread != null) {
            currentThread.interrupt();
            try {
                currentThread.join(STOP_WAIT.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void consume(String subject, String stream, String durable) {

        while (running && !Thread.currentThread().isInterrupted()) {
            JetStreamSubscription currentSubscription = null;
            try {
                currentSubscription = natsService.pullSubscribe(subject, stream, durable);
                subscription = currentSubscription;
                fetch(currentSubscription);
            } catch (Exception e) {
                if (!running || Thread.currentThread().isInterrupted()) {
                    break;
                }

                if (isInactiveSubscription(e)) {
                    log.warn("{} subscription became inactive; reconnecting", operationName);
                } else {
                    log.error("{} fetch loop failed: {}", operationName, e.getMessage(), e);
                }

                pauseBeforeReconnect();
            } finally {
                if (subscription == currentSubscription) {
                    subscription = null;
                }
                if (currentSubscription != null) {
                    safeUnsubscribe(currentSubscription);
                }
            }
        }
    }

    private void fetch(JetStreamSubscription currentSubscription) {

        while (running && !Thread.currentThread().isInterrupted()) {
            List<Message> messages = currentSubscription.fetch(BATCH_SIZE, FETCH_TIMEOUT);
            for (Message msg : messages) {
                handle(msg);
            }
        }
    }

    private void handle(Message msg) {

        try {
            T data = natsService.deserialize(msg.getData(), messageType);
            try (MdcContext.Scope ignored = MdcContext.open(mdcExtractor.apply(data))) {
                handler.handle(data);
            }
        } catch (Exception err) {
            log.error("{} failed - acking to avoid requeue: {}", operationName, err.getMessage(), err);
        } finally {
            try {
                msg.ack();
            } catch (Exception ackError) {
                log.warn("{} message ack failed: {}", operationName, ackError.getMessage());
            }
        }
    }

    private void pauseBeforeReconnect() {

        try {
            Thread.sleep(RECONNECT_WAIT.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean isInactiveSubscription(Exception e) {

        return e instanceof IllegalStateException
                   && e.getMessage() != null
                   && e.getMessage()
                       .toLowerCase()
                       .contains("subscription is inactive");
    }

    private void safeUnsubscribe(JetStreamSubscription currentSubscription) {

        try {
            currentSubscription.unsubscribe();
        } catch (Exception ignored) {
            // The subscription may already be inactive during shutdown or reconnect.
        }
    }

}
