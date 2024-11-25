package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class StreamProcessorCustomErrorHandler implements StreamsUncaughtExceptionHandler {

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.error("Exception in the application: {} ", exception.getMessage(), exception);

        if (exception instanceof StreamsException) {
            var cause = exception.getCause();
            if (cause.getMessage().equals("Transient Error")) {
                //return StreamThreadExceptionResponse.REPLACE_THREAD;
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
        }
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
