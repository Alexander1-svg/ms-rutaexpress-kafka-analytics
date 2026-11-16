package com.ms_rutaexpress.kafka_analytics.dto;

import java.util.Map;

public record StatusAnalyticsResponse(Map<String, Long> byStatus) {
}

