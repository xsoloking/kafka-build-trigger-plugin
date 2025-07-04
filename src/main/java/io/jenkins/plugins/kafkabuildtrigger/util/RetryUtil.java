package io.jenkins.plugins.kafkabuildtrigger.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.function.Predicate;

import static io.jenkins.plugins.kafkabuildtrigger.KafkaBuildTriggerConstants.*;

/**
 * Utility class for implementing retry logic.
 * 
 * @author Optimized by AI Assistant
 */
public final class RetryUtil {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryUtil.class);
    
    private RetryUtil() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
    
    /**
     * Executes a callable with retry logic.
     * 
     * @param callable the operation to execute
     * @param maxRetries maximum number of retries
     * @param delayMs delay between retries in milliseconds
     * @param retryCondition condition to determine if retry should be attempted
     * @param <T> return type
     * @return result of the callable
     * @throws Exception if all retries fail
     */
    public static <T> T executeWithRetry(
            Callable<T> callable,
            int maxRetries,
            long delayMs,
            Predicate<Exception> retryCondition) throws Exception {
        
        Exception lastException = null;
        
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return callable.call();
            } catch (Exception e) {
                lastException = e;
                
                if (attempt == maxRetries || !retryCondition.test(e)) {
                    break;
                }
                
                LOGGER.warn("Attempt {} failed, retrying in {}ms: {}", 
                           attempt + 1, delayMs, e.getMessage());
                
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted", ie);
                }
            }
        }
        
        throw lastException;
    }
    
    /**
     * Executes a callable with default retry settings.
     * 
     * @param callable the operation to execute
     * @param <T> return type
     * @return result of the callable
     * @throws Exception if all retries fail
     */
    public static <T> T executeWithDefaultRetry(Callable<T> callable) throws Exception {
        return executeWithRetry(
            callable,
            MAX_RETRY_TIMES,
            SLEEP_SECONDS * 1000L,
            e -> !(e instanceof InterruptedException)
        );
    }
    
    /**
     * Executes a runnable with retry logic.
     * 
     * @param runnable the operation to execute
     * @param maxRetries maximum number of retries
     * @param delayMs delay between retries in milliseconds
     * @param retryCondition condition to determine if retry should be attempted
     * @throws Exception if all retries fail
     */
    public static void executeWithRetry(
            Runnable runnable,
            int maxRetries,
            long delayMs,
            Predicate<Exception> retryCondition) throws Exception {
        
        executeWithRetry(() -> {
            runnable.run();
            return null;
        }, maxRetries, delayMs, retryCondition);
    }
    
    /**
     * Executes a runnable with default retry settings.
     * 
     * @param runnable the operation to execute
     * @throws Exception if all retries fail
     */
    public static void executeWithDefaultRetry(Runnable runnable) throws Exception {
        executeWithDefaultRetry(() -> {
            runnable.run();
            return null;
        });
    }
}
