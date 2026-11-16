package com.ms_rutaexpress.kafka_analytics.dto;

import java.util.List;
import java.util.Map;

public record AnalyticsSummaryResponse(
        long totalProcessed,
        long duplicateEvents,
        Map<String, Long> byStatus,
        List<RecentShipmentEventResponse> recentEvents
) {
}

