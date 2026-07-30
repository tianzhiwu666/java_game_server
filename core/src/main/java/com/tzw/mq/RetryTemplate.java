package com.tzw.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * 重试模板 — 支持指数退避的重试机制。
 */
public class RetryTemplate {

    private static final Logger log = LoggerFactory.getLogger(RetryTemplate.class);

    private final int maxRetries;
    private final long initialBackoffMs;
    private final long maxBackoffMs;

    public RetryTemplate(int maxRetries, long initialBackoffMs) {
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = 30_000;
    }

    public RetryTemplate() {
        this(3, 1000);
    }

    public <T> T execute(Supplier<T> action) {
        int attempt = 0;
        long backoff = initialBackoffMs;

        while (true) {
            try {
                return action.get();
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries) {
                    log.error("[Retry] all {} attempts failed: {}", maxRetries, e.getMessage());
                    throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
                }

                log.warn("[Retry] attempt {}/{} failed: {}, retrying in {}ms",
                        attempt, maxRetries, e.getMessage(), backoff);

                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted", ie);
                }

                backoff = Math.min(backoff * 2, maxBackoffMs);
            }
        }
    }

    public void execute(Runnable action) {
        execute(() -> {
            action.run();
            return null;
        });
    }
}
