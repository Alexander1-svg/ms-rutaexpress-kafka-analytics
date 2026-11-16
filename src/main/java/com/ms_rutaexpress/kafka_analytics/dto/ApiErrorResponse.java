package com.ms_rutaexpress.kafka_analytics.dto;

import java.time.Instant;

public record ApiErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String path
) {
}

