package com.ms_rutaexpress.kafka_analytics.service;

import com.ms_rutaexpress.kafka_analytics.dto.AnalyticsSummaryResponse;
import com.ms_rutaexpress.kafka_analytics.dto.RecentShipmentEventResponse;
import com.ms_rutaexpress.kafka_analytics.dto.StatusAnalyticsResponse;
import com.ms_rutaexpress.kafka_analytics.repository.InMemoryAnalyticsRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AnalyticsQueryService {

    private final InMemoryAnalyticsRepository repository;

    public AnalyticsQueryService(InMemoryAnalyticsRepository repository) {
        this.repository = repository;
    }

    public AnalyticsSummaryResponse summary() {
        return repository.snapshot();
    }

    public StatusAnalyticsResponse statuses() {
        return new StatusAnalyticsResponse(repository.snapshot().byStatus());
    }

    public List<RecentShipmentEventResponse> recent(int limit) {
        List<RecentShipmentEventResponse> events = repository.snapshot().recentEvents();
        return events.subList(0, Math.min(limit, events.size()));
    }
}

