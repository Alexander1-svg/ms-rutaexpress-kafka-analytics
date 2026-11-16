package com.ms_rutaexpress.kafka_analytics.exception;

public class InvalidShipmentEventException extends RuntimeException {

    public InvalidShipmentEventException(String message) {
        super(message);
    }

    public InvalidShipmentEventException(String message, Throwable cause) {
        super(message, cause);
    }
}

